import json
from datetime import datetime
from Flask_SQLAlchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.orm import clear_mappers


db = SQLAlchemy()

# Zuordnung zwischen String Namen und SQLAlchemy Python Objekten
COLUMN_TYPES = {
    "Integer": Integer,
    "String": String,
    "Float": Float,
    "DateTime": DateTime
}


def parse_column(col_definition):
    """
    Konvertiert einen String wie:
    "String(100), nullable=True, primary_key=True"
    in tatsächliche SQLAlchemy Spaltenargumente.
    """

    parts = [p.strip() for p in col_definition.split(",")]

    # --- Type Handling ---
    type_part = parts[0] # e.g. "Integer" oder "String(100)"

    if "(" in type_part:
        type_name = type_part.split("(")[0]
        size = int(type_part.split("(")[1].replace(")", ""))
        column_type = COLUMN_TYPES[type_name](size)
    else:
        type_name = type_part
        if type_name not in COLUMN_TYPES:
            raise ValueError(f"Unbekannter Typ: {type_name}")
        column_type = COLUMN_TYPES[type_part]()

    # --- Keywords arguments ---
    kwargs = {}

    for p in parts[1:]:
        # ForeignKey special case
        if p.startswith("ForeignKey"):
            kwargs["ForeignKey"] = p
            continue

        if "=" not in p:
            continue

        key, val = p.split("=")
        key = key.strip()
        val = val.strip()

        # Convert booleans
        if val == "True":
            val = True
        elif val == "False":
            val = False
        # Datetime handling
        elif val == "datetime.utcnow":
            val = datetime.now()
        # numbers
        elif val.isdigit():
            val = int(val)

        kwargs[key] = val

    return column_type, kwargs


# --------------------------------------------------------
#               2 - Phasen Modell-Generator
# --------------------------------------------------------


def generate_models(schema_path="schema.json"):
    """
    Robuster Generator: baut in einem Durchgang Klassen mit allen Columns (inkl. PK/FKs).
    Vorherige Mapper werden aufgeräumt (clear_mappers) wenn nötig.
    """

    # Falls vorher schon Mapper existieren (REPL), räume auf
    try:
        clear_mappers()
    except Exception:
        # ignore if nothing to clear / older SQLAlchemy versions
        pass

    # clean metadata (sicherstellen, dass keine alten Tabellenreste da sind)
    db.metadata.clear()

    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    models = {}

    # Wichtig: Erzeuge für jedes Model seine Attribute (inkl. Columns) und
    # rufe dann type() einmal auf — so sind PKs beim Mapping vorhanden.
    for model_name, data in schema.items():
        attrs = {
            "__tablename__": data["tablename"],
            "__module__": __name__,
        }

        # Spalten erzeugen
        for col_name, definition in data.get("columns", {}).items():
            column_type, kwargs = parse_column(definition)

            if "ForeignKey" in kwargs:
                fk_raw = kwargs.pop("ForeignKey")
                # Extract 'user.id' from "ForeignKey('user.id')"
                if "(" in fk_raw and ")" in fk_raw:
                    fk_target = fk_raw[fk_raw.index("(") + 1 : fk_raw.rindex(")")]
                    fk_target = fk_target.strip().strip("'").strip('"')
                else:
                    fk_target = fk_raw
                col = Column(column_type, ForeignKey(fk_target), **kwargs)
            else:
                col = Column(column_type, **kwargs)

            attrs[col_name] = col

        # Relationships (können Klassen-Namen referenzieren)
        for rel_name, rel_data in data.get("relationships", {}).items():
            attrs[rel_name] = relationship(
                rel_data["model"],
                backref=rel_data.get("backref"),
                lazy=True
            )

        # Klasse ein für alle Mal erzeugen (mit allen Columns)
        model_class = type(model_name, (db.Model,), attrs)
        models[model_name] = model_class

    return models
