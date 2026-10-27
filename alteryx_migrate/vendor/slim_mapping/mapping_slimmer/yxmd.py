"""Read what only the original .yxmd knows: connection numbers (#1, #2, ...) and tool-container names. Streams the file."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field


@dataclass
class YxmdInfo:
    connection_names: dict = field(default_factory=dict)   # (destination tool, source tool, source anchor) -> "#n"
    container_of: dict = field(default_factory=dict)       # tool id -> caption of its enclosing container


def read_yxmd(path: str) -> YxmdInfo:
    info = YxmdInfo()
    stack: list[dict] = []
    for event, element in ET.iterparse(path, events=("start", "end")):
        if event == "start":
            if element.tag == "Node":
                stack.append({"id": element.get("ToolID"), "plugin": "", "caption": None})
            continue
        if element.tag == "GuiSettings" and stack:
            stack[-1]["plugin"] = element.get("Plugin") or ""
        elif element.tag == "Caption" and stack and "ToolContainer" in stack[-1]["plugin"]:
            stack[-1]["caption"] = (element.text or "").strip()
        elif element.tag == "Node" and stack:
            tool = stack.pop()
            container = next((s for s in reversed(stack) if "ToolContainer" in s["plugin"] and s["caption"]), None)
            if container and tool["id"]:
                info.container_of[int(tool["id"])] = container["caption"]
            if not stack:
                element.clear()                              # keep memory flat on very large workflows
        elif element.tag == "Connection" and element.get("name"):
            origin, destination = element.find("Origin"), element.find("Destination")
            if origin is not None and destination is not None:
                key = (int(destination.get("ToolID")), int(origin.get("ToolID")), origin.get("Connection"))
                info.connection_names[key] = element.get("name")
    return info
