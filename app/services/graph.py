async def retrieve_entries(db, domain):
    cursor = db.dns.aggregate([
        {"$match": {"domain": domain}},
        {"$graphLookup": {
            "from": "dns",
            "startWith": "$ssl.subject_alt_names",
            "connectFromField": "domain",
            "connectToField": "ssl.subject_alt_names",
            "as": "certificates"
        }},
        {"$graphLookup": {
            "from": "dns",
            "startWith": "$cname_record.target",
            "connectFromField": "domain",
            "connectToField": "cname_record.target",
            "as": "cname_records"
        }},
        {"$graphLookup": {
            "from": "dns",
            "startWith": "$mx_record.exchange",
            "connectFromField": "domain",
            "connectToField": "domain",
            "as": "mx_records"
        }},
        {"$graphLookup": {
            "from": "dns",
            "startWith": "$ns_record",
            "connectFromField": "domain",
            "connectToField": "domain",
            "as": "ns_records"
        }},
        {"$project": {
            "main.domain": "$domain",
            "main.a_record": "$a_record",
            "main.subject_alt_names": "$ssl.subject_alt_names",
            "zzz": {
                "$setUnion": [
                    "$certificates",
                    "$cname_records",
                    "$mx_records",
                    "$ns_records"
                ]
            }
        }},
        {"$unwind": "$zzz"},
        {"$group": {
            "_id": "$_id",
            "main": {"$addToSet": "$main"},
            "all": {"$addToSet": "$zzz"}
        }},
        {"$project": {
            "all.domain": 1,
            "all.a_record": 1,
            "all.ssl.subject_alt_names": 1,
            "main": 1,
            "_id": 0
        }}
    ])
    return await cursor.to_list(length=None)


def update_summary(summary, data):
    domain = data.get('domain')
    if not domain:
        return summary

    entry = summary.get(domain)
    if entry is None:
        entry = {
            "a_records": set(),
            "subject_alt_names": set(),
            "alias_only": True,
        }
        summary[domain] = entry

    entry["alias_only"] = False

    a_records = data.get('a_record') or []
    if isinstance(a_records, str):
        a_records = [a_records]
    entry["a_records"].update(a_records)

    alt_names = data.get('subject_alt_names')
    if alt_names is None:
        alt_names = data.get('ssl', {}).get('subject_alt_names', [])
    if isinstance(alt_names, str):
        alt_names = [alt_names]

    cleaned_alt_names = [alt for alt in alt_names if alt]
    entry["subject_alt_names"].update(cleaned_alt_names)

    for alt in cleaned_alt_names:
        alt_entry = summary.get(alt)
        if alt_entry is None:
            summary[alt] = {
                "a_records": set(),
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

    groups = sorted({ip for info in summary.values() for ip in info["a_records"]})
    groups_d = {ip: idx for idx, ip in enumerate(groups)}

    nodes = []
    node_ids = {}

    for idx, label in enumerate(sorted(summary)):
        info = summary[label]
        node_ids[label] = idx

        node = {"id": idx, "label": label}

        if info["a_records"]:
            first_ip = sorted(info["a_records"])[0]
            node["group"] = str(groups_d[first_ip])

        if info["subject_alt_names"]:
            node["subject_alt_names"] = sorted(info["subject_alt_names"])

        if info.get("alias_only"):
            node["alias_only"] = True

        nodes.append(node)

    edges_s = set()

    for label, info in summary.items():
        from_id = node_ids[label]
        for alt in info["subject_alt_names"]:
            if alt not in node_ids:
                continue
            to_id = node_ids[alt]
            if from_id == to_id:
                continue
            if (to_id, from_id) not in edges_s:
                edges_s.add((from_id, to_id))

    main_ids = {node_ids[d] for d in main_domains if d in node_ids}

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
