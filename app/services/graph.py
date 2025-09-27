"""PostgreSQL-backed helpers for building domain relationship graphs."""

from __future__ import annotations

import ipaddress
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import func, select
from sqlalchemy.orm import selectinload
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.postgres import (
    ARecord,
    AAAARecord,
    CNAMERecord,
    Domain,
    MXRecord,
    SoaRecord,
    SSLData,
    SSLSubjectAltName,
)


def _domain_query():
    return (
        select(Domain)
                .options(
            selectinload(Domain.a_records),
            selectinload(Domain.aaaa_records),
            selectinload(Domain.mx_records),
            selectinload(Domain.cname_records),
            selectinload(Domain.ssl).selectinload(SSLData.subject_alt_names),
        )
    )


def _serialize_domain(domain: Domain) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "domain": domain.name,
        "a_record": [record.ip_address for record in domain.a_records],
        "aaaa_record": [record.ip_address for record in domain.aaaa_records],
        "mx_record": [
            {"exchange": record.exchange, "priority": record.priority}
            for record in domain.mx_records
        ],
    }

    ssl_payload = domain.ssl
    if ssl_payload:
        payload["ssl"] = {
            "subject_alt_names": [
                san.value for san in ssl_payload.subject_alt_names or []
            ]
        }

    return payload


async def _load_related(session: AsyncSession, expr) -> List[Domain]:
    stmt = _domain_query().where(expr)
    result = await session.exec(stmt)
    return result.scalars().unique().all()


def _ensure_node(
    nodes: Dict[str, Dict[str, Any]], label: str
) -> Dict[str, Any]:
    if label not in nodes:
        nodes[label] = {
            "id": label,
            "label": label,
            "type": _classify_entity(label),
        }
    return nodes[label]


def _classify_entity(entity: str) -> str:
    try:
        addr = ipaddress.ip_address(entity)
        return "ipv6" if addr.version == 6 else "ipv4"
    except ValueError:
        return "domain"


async def build_domain_graph(
    session: AsyncSession,
    domain: str,
) -> Dict[str, Any]:
    stmt = (
        _domain_query()
        .where(func.lower(Domain.name) == domain.lower())
        .limit(1)
    )
    result = await session.exec(stmt)
    main = result.scalars().unique().one_or_none()
    if not main:
        return {"nodes": [], "edges": []}

    main_payload = _serialize_domain(main)

    related: Dict[int, Domain] = {}

    async def gather(expr) -> None:
        domains = await _load_related(session, expr)
        for dom in domains:
            if dom.id == main.id:
                continue
            related[dom.id] = dom

    ipv4s = main_payload.get("a_record", [])
    if ipv4s:
        await gather(Domain.a_records.any(ARecord.ip_address.in_(ipv4s)))

    ipv6s = main_payload.get("aaaa_record", [])
    if ipv6s:
        await gather(Domain.aaaa_records.any(AAAARecord.ip_address.in_(ipv6s)))

    mx_hosts = [entry["exchange"] for entry in main_payload.get(
        "mx_record", []) if entry.get("exchange")]
    if mx_hosts:
        await gather(Domain.mx_records.any(MXRecord.exchange.in_(mx_hosts)))

    cname_targets = [record.target for record in main.cname_records]
    if cname_targets:
        await gather(
            Domain.cname_records.any(CNAMERecord.target.in_(cname_targets))
        )

    alt_names = main_payload.get("ssl", {}).get(
        "subject_alt_names", []) if main_payload.get("ssl") else []
    if alt_names:
        await gather(
            Domain.ssl.has(
                SSLData.subject_alt_names.any(
                    SSLSubjectAltName.value.in_(alt_names))
            )
        )

    nodes: Dict[str, Dict[str, Any]] = {}
    edges: Set[Tuple[str, str, str, Optional[int]]] = set()

    def populate(payload: Dict[str, Any]) -> None:
        label = payload["domain"]
        node = _ensure_node(nodes, label)
        node["type"] = "domain"

        for ip in payload.get("a_record", []) or []:
            _ensure_node(nodes, ip)
            edges.add((label, ip, "a_record", None))
        for ip in payload.get("aaaa_record", []) or []:
            _ensure_node(nodes, ip)
            edges.add((label, ip, "aaaa_record", None))
        for entry in payload.get("mx_record", []) or []:
            exchange = entry.get("exchange") if isinstance(
                entry, dict) else None
            priority = entry.get("priority") if isinstance(
                entry, dict) else None
            if exchange:
                _ensure_node(nodes, exchange)
                edges.add((label, exchange, "mx_record", priority))

        ssl_info = payload.get("ssl") or {}
        for san in ssl_info.get("subject_alt_names", []) or []:
            _ensure_node(nodes, san)
            edges.add((label, san, "ssl_alt_name", None))

    populate(main_payload)
    for dom in related.values():
        populate(_serialize_domain(dom))

    edge_payload = []
    
    # Sort edges with None priorities treated as highest value (last)
    def sort_key(edge):
        src, dst, edge_type, priority = edge
        safe_priority = priority if priority is not None else float('inf')
        return (src, dst, edge_type, safe_priority)
    
    for src, dst, edge_type, priority in sorted(edges, key=sort_key):
        entry = {"source": src, "target": dst, "type": edge_type}
        if priority is not None:
            entry["priority"] = priority
        edge_payload.append(entry)

    return {
        "nodes": list(nodes.values()),
        "edges": edge_payload,
    }


__all__ = ["build_domain_graph"]
