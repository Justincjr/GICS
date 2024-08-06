from typing_extensions import TypedDict, NotRequired

class GICSLevel(TypedDict):
    code: int
    description: str

class GICSCode(TypedDict):
    level1: GICSLevel
    level2: GICSLevel
    level3: GICSLevel
    level4: GICSLevel