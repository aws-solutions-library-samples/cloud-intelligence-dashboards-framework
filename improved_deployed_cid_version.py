# Improved version of deployed_cid_version method
from cid.helpers.quicksight.version import DEFAULT_VERSION

# Constants at class level
INVALID_TAG_VERSIONS = {DEFAULT_VERSION}  # Only v0.0.0 is universally invalid
DASHBOARDS_WITH_INVALID_V1_TAGS = {'ta-organizational-view', 'resiliencevue'}  # v1.0.0 invalid only for these

@property
def deployed_cid_version(self):
    """Get the deployed CID version from tags, template, or definition."""
    if self._cid_version:
        return self._cid_version
    
    try:
        tag_version = self._get_version_from_tags()
        if self._is_valid_tag_version(tag_version):
            logger.debug(f'Using version from tag for {self.id}: {tag_version}')
            self._cid_version = CidVersion(tag_version)
        else:
            self._cid_version = self._get_version_from_sources()
            if self._cid_version:
                self._update_version_tag()
    except Exception as exc:
        logger.warning(f'Failed to determine CID version for {self.id}: {exc}')
        self._cid_version = None
    
    return self._cid_version

def _get_version_from_tags(self):
    """Extract version from dashboard tags."""
    tags = self.qs.get_tags(self.arn) or {}
    return tags.get('cid_version_tag')

def _is_valid_tag_version(self, tag_version):
    """Check if tag version is valid and not a known invalid default."""
    if not tag_version:
        return False
    
    # v0.0.0 is universally invalid (default when version can't be determined)
    if tag_version in self.INVALID_TAG_VERSIONS:
        return False
    
    # v1.0.0 is invalid only for specific dashboards with incorrect defaults
    if (tag_version == 'v1.0.0' and 
        self.id in self.DASHBOARDS_WITH_INVALID_V1_TAGS):
        logger.debug(f'Ignoring invalid v1.0.0 tag for dashboard {self.id}')
        return False
    
    return True

def _get_version_from_sources(self):
    """Get version from template or definition as fallback."""
    if self.deployed_template:
        return self.deployed_template.cid_version
    elif self.deployed_definition:
        return self.deployed_definition.cid_version
    return None

def _update_version_tag(self):
    """Update the version tag if we found a version from other sources."""
    try:
        logger.debug(f'Setting version tag for {self.arn}: {self._cid_version}')
        self.qs.set_tags(self.arn, cid_version_tag=self._cid_version)
    except Exception as exc:
        logger.warning(f'Failed to update version tag for {self.id}: {exc}')