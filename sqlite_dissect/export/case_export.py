import json
import uuid

from _version import __version__ 

"""

case_export.py

This script holds the objects used for exporting information regarding SQLite carving process to CASE format. 
Information about the CASE Cyber Ontology can be found at: https://caseontology.org/


"""

class CaseExporter(object):

    case = {
        '@context': {},
        '@graph': {}
    }


    def generate_header():
        case['@context'] = {
            "@vocab": "http://example.org/ontology/local#",
            "case-investigation": "https://ontology.caseontology.org/case/investigation/",
            "drafting": "http://example.org/ontology/drafting#",
            "kb": "http://example.org/kb/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "uco-action": "https://unifiedcyberontology.org/ontology/uco/action#",
            "uco-core": "https://unifiedcyberontology.org/ontology/uco/core#",
            "uco-identity": "https://unifiedcyberontology.org/ontology/uco/identity#",
            "uco-location": "https://unifiedcyberontology.org/ontology/uco/location#",
            "uco-observable": "https://unifiedcyberontology.org/ontology/uco/observable#",
            "uco-tool": "https://unifiedcyberontology.org/ontology/uco/tool#",
            "uco-types": "https://unifiedcyberontology.org/ontology/uco/types#",
            "uco-vocabulary": "https://unifiedcyberontology.org/ontology/uco/vocabulary#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        }


    def generate_tool_signature():
        case['@graph'].append({
            "@id": ("kb:sqlite-dissect" + str(uuid.uuid4())),
            "@type": "uco-tool:Tool",
            "uco-core:name": "SQLite Dissect",
            "uco-tool:toolType": "Extraction",
            "uco-tool:creator": "Defense Cyber Crime Center (DC3)",
            "uco-tool:version": __version__,
        })


    def export_case_file():
        with open('case.json', 'w', encoding='utf-8') as f:
            json.dump(case, f, ensure_ascii=False, indent=4)