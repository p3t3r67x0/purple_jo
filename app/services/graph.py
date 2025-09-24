async def retrieve_entries(db, domain):
    """
    Retrieve graph entries for a domain, finding all related domains
    that share the same A records.
    """
    print(f"DEBUG: Searching for domain: {domain}")

    # Find the main domain
    main_domain = await db.dns.find_one({"domain": domain})
    main_domain_name = main_domain.get('domain') if main_domain else None
    print(f"DEBUG: Found main domain: {main_domain_name}")

    if not main_domain:
        print(f"DEBUG: Domain {domain} not found in database")
        return []

    # Extract A records from the main domain
    a_records = main_domain.get("a_record", [])
    print(f"DEBUG: A records for {domain}: {a_records}")

    if not a_records:
        print(f"DEBUG: No A records found for {domain}")
        # Convert ObjectId to string for JSON serialization
        main_clean = {k: (str(v) if k == "_id" else v)
                      for k, v in main_domain.items()}
        return [main_clean]

    # Find all domains that share any of these A records
    related_domains = []
    async for doc in db.dns.find({"a_record": {"$in": a_records}}):
        related_domains.append(doc)

    print(f"DEBUG: Found {len(related_domains)} related domains")

    # Clean up ObjectId fields for JSON serialization
    clean_domains = []
    for doc in related_domains:
        clean_doc = {k: (str(v) if k == "_id" else v) for k, v in doc.items()}
        clean_domains.append(clean_doc)
        print(f"DEBUG: Related domain: {doc.get('domain')}")

    # Convert main domain for consistency
    main_clean = {k: (str(v) if k == "_id" else v)
                  for k, v in main_domain.items()}

    return clean_domains


def update_summary(summary, data):
    domain = data.get('domain')
    if not domain:
        return summary

    entry = summary.get(domain)
    if entry is None:
        entry = {
            "a_records": set(),
            "aaaa_records": set(),
            "mx_records": set(),
            "ns_records": set(),
            "subject_alt_names": set(),
            "alias_only": True,
        }
        summary[domain] = entry

    entry["alias_only"] = False

    # Handle A records
    a_records = data.get('a_record') or []
    if isinstance(a_records, str):
        a_records = [a_records]
    entry["a_records"].update(a_records)

    # Handle AAAA records
    aaaa_records = data.get('aaaa_record') or []
    if isinstance(aaaa_records, str):
        aaaa_records = [aaaa_records]
    entry["aaaa_records"].update(aaaa_records)

    # Handle MX records
    mx_records = data.get('mx_record') or []
    if isinstance(mx_records, dict):
        mx_records = [mx_records]
    mx_exchanges = [
        mx.get('exchange') for mx in mx_records
        if isinstance(mx, dict) and mx.get('exchange')
    ]
    entry["mx_records"].update(mx_exchanges)

    # Handle NS records
    ns_records = data.get('ns_record') or []
    if isinstance(ns_records, str):
        ns_records = [ns_records]
    entry["ns_records"].update(ns_records)

    # Handle SSL subject alternative names
    alt_names = data.get('subject_alt_names')
    if alt_names is None:
        alt_names = data.get('ssl', {}).get('subject_alt_names', [])
    if isinstance(alt_names, str):
        alt_names = [alt_names]

    cleaned_alt_names = [alt for alt in alt_names if alt]
    entry["subject_alt_names"].update(cleaned_alt_names)

    # Create alias entries for subject alternative names
    for alt in cleaned_alt_names:
        alt_entry = summary.get(alt)
        if alt_entry is None:
            summary[alt] = {
                "a_records": set(),
                "aaaa_records": set(),
                "mx_records": set(),
                "ns_records": set(),
                "subject_alt_names": set(),
                "alias_only": True,
            }

    # Create alias entries for MX and NS record targets
    for mx_exchange in mx_exchanges:
        if mx_exchange and mx_exchange not in summary:
            summary[mx_exchange] = {
                "a_records": set(),
                "aaaa_records": set(),
                "mx_records": set(),
                "ns_records": set(),
                "subject_alt_names": set(),
                "alias_only": True,
            }

    for ns_record in ns_records:
        if ns_record and ns_record not in summary:
            summary[ns_record] = {
                "a_records": set(),
                "aaaa_records": set(),
                "mx_records": set(),
                "ns_records": set(),
                "subject_alt_names": set(),
                "alias_only": True,
            }

    return summary


async def extract_graph(db, domain):
    results = await retrieve_entries(db, domain)
    if not results:
        return {}

    summary = {}
    main_domains = set()

    for item in results[0].get('main', []):
        main_domains.add(item.get('domain'))
        update_summary(summary, item)

    for item in results[0].get('all', []):
        update_summary(summary, item)

    if not summary:
        return {}

    # Create groups from all IP addresses (A and AAAA records)
    all_ips = set()
    for info in summary.values():
        all_ips.update(info["a_records"])
        all_ips.update(info["aaaa_records"])
    groups = sorted(all_ips)
    groups_d = {ip: idx for idx, ip in enumerate(groups)}

    nodes = []
    node_ids = {}

    for idx, label in enumerate(sorted(summary)):
        info = summary[label]
        node_ids[label] = idx

        node = {"id": idx, "label": label}

        # Set group based on first IP address (A or AAAA)
        all_node_ips = list(info["a_records"]) + list(info["aaaa_records"])
        if all_node_ips:
            first_ip = sorted(all_node_ips)[0]
            node["group"] = str(groups_d[first_ip])

        # Include all DNS record types in node data
        if info["a_records"]:
            node["a_records"] = sorted(info["a_records"])

        if info["aaaa_records"]:
            node["aaaa_records"] = sorted(info["aaaa_records"])

        if info["mx_records"]:
            node["mx_records"] = sorted(info["mx_records"])

        if info["ns_records"]:
            node["ns_records"] = sorted(info["ns_records"])

        if info["subject_alt_names"]:
            node["subject_alt_names"] = sorted(info["subject_alt_names"])

        if info.get("alias_only"):
            node["alias_only"] = True

        nodes.append(node)

    edges_s = set()

    # Create edges from SSL subject alternative names
    for label, info in summary.items():
        from_id = node_ids[label]

        # SSL subject alternative names relationships
        for alt in info["subject_alt_names"]:
            if alt not in node_ids:
                continue
            to_id = node_ids[alt]
            if from_id == to_id:
                continue
            if (to_id, from_id) not in edges_s:
                edges_s.add((from_id, to_id))

        # MX record relationships (domain -> mail exchange)
        for mx_exchange in info["mx_records"]:
            if mx_exchange not in node_ids:
                continue
            to_id = node_ids[mx_exchange]
            if from_id == to_id:
                continue
            if (to_id, from_id) not in edges_s:
                edges_s.add((from_id, to_id))

        # NS record relationships (domain -> nameserver)
        for ns_record in info["ns_records"]:
            if ns_record not in node_ids:
                continue
            to_id = node_ids[ns_record]
            if from_id == to_id:
                continue
            if (to_id, from_id) not in edges_s:
                edges_s.add((from_id, to_id))

    # Create relationships based on shared IP addresses (A and AAAA records)
    ip_to_domains = {}

    # Build IP to domains mapping
    for label, info in summary.items():
        if label not in node_ids:
            continue
        domain_id = node_ids[label]

        # Map A records (IPv4)
        for ip in info["a_records"]:
            if ip not in ip_to_domains:
                ip_to_domains[ip] = []
            ip_to_domains[ip].append(domain_id)

        # Map AAAA records (IPv6)
        for ip in info["aaaa_records"]:
            if ip not in ip_to_domains:
                ip_to_domains[ip] = []
            ip_to_domains[ip].append(domain_id)

    # Create edges between domains that share IP addresses
    for ip, domain_ids in ip_to_domains.items():
        if len(domain_ids) > 1:
            # Create edges between all domains sharing this IP
            for i, from_id in enumerate(domain_ids):
                for to_id in domain_ids[i+1:]:
                    if from_id != to_id:
                        edge_exists = (
                            (to_id, from_id) in edges_s or
                            (from_id, to_id) in edges_s
                        )
                        if not edge_exists:
                            edges_s.add((from_id, to_id))
            if from_id == to_id:
                continue
            if (to_id, from_id) not in edges_s:
                edges_s.add((from_id, to_id))

    main_ids = {node_ids[d] for d in main_domains if d in node_ids}

    # Connect main domains to related nodes
    for node in nodes:
        for main_id in main_ids:
            if node["id"] == main_id:
                continue
            if (node["id"], main_id) not in edges_s:
                edges_s.add((main_id, node["id"]))

    edges = [{"from": src, "to": dst} for src, dst in sorted(edges_s)]

    if nodes and edges:
        return {"nodes": nodes, "edges": edges}
    return {}
