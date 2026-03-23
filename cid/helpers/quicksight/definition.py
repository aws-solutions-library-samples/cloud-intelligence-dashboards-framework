import logging
import re
from typing import Dict
from cid.helpers.quicksight.resource import CidQsResource
from cid.helpers.quicksight.version import CidVersion, DEFAULT_VERSION

logger = logging.getLogger(__name__)

class Definition:

    def __init__(self, raw: dict) -> None:
        self.raw: dict = raw
        # Resolve version from definition contents
        self._raw_version = self.resolve_version(self.raw)
    
    @property
    def cid_version(self) -> CidVersion:
        # Resolve version from "About" sheet contents
        try:
            return CidVersion(self._raw_version)
        except TypeError as e:
            logger.debug(f"Could not resolve CID version. Raw version value '{self._raw_version}' does not conform to CID version format vmajor.minor.build e.g. v.1.0.1")
        
        return CidVersion(DEFAULT_VERSION)
    
    def resolve_version(self, raw: dict):
        about_content = []

        sheets = raw.get("Sheets", [])
        
        if sheets:
            text_boxes_content = self._extract_sheet_textboxes_content(sheets)
            about_content += text_boxes_content
            
            insight_visuals_content = self._extract_sheet_visuals_content(sheets)
            about_content += insight_visuals_content

        if about_content:
            all_about_content = " | ".join(about_content)
            # find first string that looks like vx.y.z using a regular expression where x, y and z are numbers
            version_matches = re.findall(r"([vV]\d+?\.\d+?\.\d+?)", all_about_content)
            if version_matches:
                return version_matches[0].lower()  # normalize to lowercase
            else:
                version_matches = re.findall(r"([vV]\d+?\.\d+?)", all_about_content)
                if version_matches:
                    return f"{version_matches[0].lower()}.0"  # normalize to lowercase

        return None
    
    def _extract_sheet_visuals_content(self, sheets: list):
        insight_visuals_content = []
        visuals = (visual for sheet in sheets for visual in sheet.get("Visuals", []) if sheet.get("Name", None) == "About")
        insight_visuals_content = [
            visual["InsightVisual"]["InsightConfiguration"]["CustomNarrative"].get("Narrative", "")
            for visual in visuals
            if "InsightVisual" in visual
            and "InsightConfiguration" in visual["InsightVisual"]
            and "CustomNarrative" in visual["InsightVisual"]["InsightConfiguration"]
        ]
        return insight_visuals_content

    def _extract_sheet_textboxes_content(self, sheets: list):
        text_boxes = (text_boxes for sheet in sheets for text_boxes in sheet.get("TextBoxes", []) if sheet.get("Name", None) == "About")
        text_boxes_content = [
            text_content.get("Content", "") for text_content in text_boxes
        ]
        return text_boxes_content