"""Parse individual Alteryx tool nodes from XML."""

from __future__ import annotations

import logging
import re

from lxml import etree

from a2d.parser.schema import PLUGIN_NAME_MAP, ParsedNode
from a2d.utils.xml_helpers import element_to_dict, get_attr

logger = logging.getLogger("a2d.parser.node")

# Pattern-based recognition for third-party / versioned plugins not in PLUGIN_NAME_MAP.
# Maps a regex on the plugin name to (tool_type, category).
_PLUGIN_PATTERNS: list[tuple[re.Pattern[str], str, str]] = [
    (re.compile(r"PublishToTableauServer", re.IGNORECASE), "PublishToTableauServer", "connectors"),
    (re.compile(r"Salesforce", re.IGNORECASE), "SalesforceConnector", "connectors"),
    (re.compile(r"SnowflakeOutput|SnowflakeInput", re.IGNORECASE), "SnowflakeConnector", "connectors"),
    (re.compile(r"PowerBI", re.IGNORECASE), "PowerBIConnector", "connectors"),
    (re.compile(r"GoogleAnalytics|GoogleBigQuery|GoogleSheets", re.IGNORECASE), "GoogleConnector", "connectors"),
    # Versioned SDK plugins (name format ToolName_<major>_<minor>_<patch>)
    (re.compile(r"^DataverseInput(_\d+)*$", re.IGNORECASE), "DataverseInput", "connectors"),
]

# Known Alteryx built-in macros (EngineSettings Macro="...") that are functionally
# equivalent to a native tool with a real converter — mapped straight to that tool's
# type instead of the generic "Macro" bucket below.
_MACRO_TOOL_MAP: dict[str, tuple[str, str]] = {
    "randomrecords.yxmc": ("Sample", "preparation"),
    "ets.yxmc": ("ETS", "predictive"),
    "ts_forecast.yxmc": ("TSForecast", "predictive"),
}


class NodeParser:
    """Parses <Node> XML elements into ParsedNode objects."""

    def parse(self, node_element: etree._Element) -> ParsedNode:
        """Parse a single <Node> XML element."""
        tool_id = int(get_attr(node_element, "ToolID", "0"))

        # Extract plugin name from GuiSettings
        gui_settings = node_element.find("GuiSettings")
        plugin_name = ""
        position = (0.0, 0.0)

        if gui_settings is not None:
            plugin_name = get_attr(gui_settings, "Plugin", "")
            pos_elem = gui_settings.find("Position")
            if pos_elem is not None:
                x = float(get_attr(pos_elem, "x", "0"))
                y = float(get_attr(pos_elem, "y", "0"))
                position = (x, y)

        # Extract configuration (needed below to detect a <MacroPath> reference
        # before we've decided the node is genuinely unresolvable).
        configuration: dict = {}
        props = node_element.find("Properties")
        if props is not None:
            config_elem = props.find("Configuration")
            if config_elem is not None:
                configuration = element_to_dict(config_elem)

        # Resolve tool type: exact match first, then pattern-based for third-party plugins
        tool_type, category = PLUGIN_NAME_MAP.get(plugin_name, ("Unknown", "unknown"))
        if tool_type == "Unknown" and plugin_name:
            for pattern, ptype, pcat in _PLUGIN_PATTERNS:
                if pattern.search(plugin_name):
                    tool_type, category = ptype, pcat
                    break

        # If still unknown and no plugin name, this tool was serialized as a bare
        # macro reference (EngineSettings Macro="...") rather than a native
        # Plugin= — common for Alteryx tools that ship as .yxmc macros (built-in,
        # like RandomRecords/ETS/TSForecast, or a project-specific custom macro).
        # A handful of built-ins map straight to their real, fully-supported tool
        # type; everything else gets the honest, distinct "Macro" tool_type (never
        # silently "Unknown") so it's reported and can be inlined via
        # --expand-macros instead of vanishing into an unlabeled unsupported node.
        if tool_type == "Unknown" and not plugin_name:
            engine_settings = node_element.find("EngineSettings")
            if engine_settings is not None:
                macro_path = get_attr(engine_settings, "Macro", "")
                if macro_path:
                    macro_file = macro_path.lower().replace("\\", "/").split("/")[-1]
                    tool_type, category = _MACRO_TOOL_MAP.get(macro_file, ("Macro", "macro"))
                    plugin_name = f"macro:{macro_file}"

        # Same idea when a Plugin= IS present but unrecognized (e.g. a
        # custom macro GUI wrapper like "CustomMacroGui.CleanseMacro") — if the
        # configuration carries an explicit <MacroPath>, this is a macro call
        # too, just serialized a third way. Keep the original plugin_name (it's
        # informative), just stop reporting it as "Unknown".
        if tool_type == "Unknown" and plugin_name:
            macro_path_cfg = configuration.get("MacroPath", "")
            if isinstance(macro_path_cfg, dict):
                macro_path_cfg = macro_path_cfg.get("#text", "")
            if isinstance(macro_path_cfg, str) and macro_path_cfg.strip():
                tool_type, category = "Macro", "macro"

        if tool_type == "Unknown":
            logger.warning("Unknown plugin: %s (ToolID=%d)", plugin_name, tool_id)

        # Extract annotation
        annotation = None
        if props is not None:
            anno_elem = props.find("Annotation")
            if anno_elem is not None:
                display_name = anno_elem.find("Name")
                if display_name is not None and display_name.text:
                    annotation = display_name.text
                elif anno_elem.text and anno_elem.text.strip():
                    annotation = anno_elem.text.strip()

        # Preserve raw XML
        raw_xml = etree.tostring(node_element, encoding="unicode")

        return ParsedNode(
            tool_id=tool_id,
            plugin_name=plugin_name,
            tool_type=tool_type,
            category=category,
            position=position,
            configuration=configuration,
            annotation=annotation,
            raw_xml=raw_xml,
        )
