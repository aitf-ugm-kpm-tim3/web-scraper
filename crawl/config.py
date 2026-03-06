from pathlib import Path

# Central configuration for Indonesian regulations (peraturan.go.id)
# Key: The identifier used for filenames and URL slugs (where applicable)
# Value: The relative path for the rekapitulasi page
PERATURAN_CONFIG = {
    "uu": "uu/rekapitulasi",
    "perppu": "perppu/rekapitulasi",
    "pp": "pp/rekapitulasi",
    "perpres": "perpres/rekapitulasi",
    "penpres": "penpres/rekapitulasi",
    "keppres": "keppres/rekapitulasi",
    "inpres": "inpres/rekapitulasi",
    "perbagi": "perban/perbagi",
    "peraturan-kpk": "perban/peraturan-kpk",
    "permenkominfo": "permen/permenkominfo",
    "permenkomdigi": "permen/permenkomdigi"
}

def get_rekapitulasi_filename(name):
    """Returns the filename for a rekapitulasi JSON file."""
    return f"peraturan_go_id_rekapitulasi_{name}.json"

def get_all_extracted_filename(name):
    """Returns the filename for an extracted 'all' JSON file."""
    return f"peraturan_go_id_all_{name}.json"

def get_metadata_filename(name):
    """Returns the filename for a metadata JSON file."""
    return f"peraturan_go_id_metadata_{name}.json"
