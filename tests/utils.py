from typing import Any, Dict


class InMemoryCollection:
    def __init__(self):
        self.docs: list[Dict[str, Any]] = []
        self.indexes = set()

    async def create_index(self, *args, **kwargs):  # noqa: D401
        # Simulate index creation (no-op)
        if args:
            self.indexes.add(args[0])

    async def find_one_and_update(
        self, key, update, upsert=False, return_document=False
    ):
        # Find matching doc
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in key.items()):
                # Apply $inc
                if "$inc" in update:
                    for k, v in update["$inc"].items():
                        doc[k] = doc.get(k, 0) + v
                if "$setOnInsert" in update:
                    # Only applies if inserted but we already had doc
                    pass
                return doc
        if upsert:
            new_doc = {**key}
            if "$inc" in update:
                for k, v in update["$inc"].items():
                    new_doc[k] = v
            if "$setOnInsert" in update:
                for k, v in update["$setOnInsert"].items():
                    new_doc[k] = v
            self.docs.append(new_doc)
            return new_doc
        return None

    async def insert_one(self, doc: Dict[str, Any]):
        self.docs.append(doc)
        return type("InsertResult", (), {"inserted_id": len(self.docs)})()


class InMemoryMongo:
    def __init__(self):
        self.contact_rate_limit = InMemoryCollection()
        self.contact_messages = InMemoryCollection()


def build_test_mongo():
    return InMemoryMongo()
