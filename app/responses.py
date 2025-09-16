import json
from datetime import datetime
from bson import ObjectId
from fastapi.responses import JSONResponse


class MongoJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        def convert(o):
            if isinstance(o, ObjectId):
                return str(o)
            if isinstance(o, datetime):
                return o.isoformat()
            raise TypeError(f"Type {type(o)} not serializable")
        return json.dumps(content, default=convert).encode("utf-8")
