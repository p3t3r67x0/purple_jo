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
            "main": 1,
            "_id": 0
        }}
    ])
    return await cursor.to_list(length=None)


def update_summary(summary, data):
    if 'a_record' in data:
        summary.add((data['domain'], ','.join(data['a_record'])))
    else:
        summary.add((data['domain'], ''))
    return summary


async def extract_graph(db, domain):
    results = await retrieve_entries(db, domain)
    summary_s = set()
    edges_s = set()
    main_s = set()
    groups = set()
    groups_d = {}
    nodes = []
    edges = []

    if results:
        for i in results[0]['main']:
            other = update_summary(summary_s, i)
            summary_s |= other
            if 'a_record' in i:
                groups.update(i['a_record'])

        for i in results[0]['all']:
            other = update_summary(summary_s, i)
            summary_s |= other
            if 'a_record' in i:
                groups.update(i['a_record'])

        for idx, v in enumerate(groups):
            groups_d[v] = idx

        for idx, v in enumerate(summary_s):
            node = {"id": idx, "label": v[0]}
            e = {"from": idx}
            if v[0] == domain:
                main_s.add(idx)

            for j in v[1].split(','):
                if j in groups:
                    e["to"] = groups_d[j]
                    if e["from"] != e["to"] and (e["to"], e["from"]) not in edges_s:
                        edges_s.add((e["from"], e["to"]))
                        node["group"] = str(groups_d[j])
            nodes.append(node)

    for v in nodes:
        for j in main_s:
            e = {"from": j, "to": v["id"]}
            if e["from"] != e["to"] and (e["to"], e["from"]) not in edges_s:
                edges_s.add((e["from"], e["to"]))

    for i in edges_s:
        edges.append({"from": i[0], "to": i[1]})

    if nodes and edges:
        return {"nodes": nodes, "edges": edges}
    return {}
