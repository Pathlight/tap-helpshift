import json
from dateutil.parser import parse as parse_datetime


def build_schema(response):

    def format_prop_type(v):
        if isinstance(v, str):
            try:
                parse_datetime(v)
                prop = {"type": ["null", "string"], "format": "date-time"}
            except:
                prop = {"type": ["null", "string"]}
        elif isinstance(v, bool):
            prop = {"type": ["null", "boolean"]}
        elif isinstance(v, int):
            prop = {"type": ["null", "integer"]}
        else:
            prop = {"type": ["null", "???"]}

        return prop

    def create_props(obj):
        properties = {}

        for k, v in obj.items():
            if isinstance(v, dict):
                properties[k] = {"type": ["null", "object"], "additionalProperties": False, "properties": create_props(v)}
            elif isinstance(v, list):
                if v:
                    example = v[0]
                    if isinstance(v, dict):
                        properties[k] = {"type": ["null", "array"], "items": create_props(example)}
                    else:
                        prop = format_prop_type(example)
                        if prop:
                            properties[k] = {"type": ["null", "array"], "items": format_prop_type(example)}
                        else:
                            continue
                else:
                    continue
            else:
                prop = format_prop_type(v)
                if prop:
                    properties[k] = prop
                else:
                    continue

        return properties

    schema = {
        "type": [
            "null",
            "object"
        ],
        "additionalProperties": False,
        "properties": {}
    }

    for row in response:
        schema['properties'].update(create_props(row))

    print(json.dumps(schema, indent=2))
