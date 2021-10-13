import hashlib
import json
import logging
import uuid
from datetime import datetime
from os import path

from _version import __version__

"""
This script holds the objects used for exporting information regarding SQLite carving process to CASE format. 
Information about the CASE Cyber Ontology can be found at: https://caseontology.org/
"""


class CaseExporter(object):
    case = {
        '@context': {},
        '@graph': []
    }
    start_datetime = None
    end_datetime = None

    def add_observable_file(self, filepath):
        if path.exists(filepath) and path.isfile(filepath):
            # Get the full path we need for reference
            filepath = path.abspath(filepath)

            # Parse the file and get the attributes we need
            self.case['@graph'].append({
                "@id": ("kb:" + str(uuid.uuid4())),
                "@type": "uco-observable:ObservableObject",
                "uco-core:hasFacet": [
                    {
                        "@type": "uco-observable:FileFacet",
                        "uco-observable:observableCreatedTime": {
                            "@type": "xsd:dateTime",
                            "@value": datetime.fromtimestamp(path.getctime(filepath)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        },
                        "uco-observable:extension": path.splitext(filepath)[1][1:],
                        "uco-observable:fileName": path.basename(filepath),
                        "uco-observable:filePath": filepath,
                        "uco-observable:isDirectory": 'false',
                        "uco-observable:sizeInBytes": path.getsize(filepath)
                    },
                    {
                        "@type": "uco-observable:ContentDataFacet",
                        "uco-observable:hash": [
                            {
                                "@type": "uco-types:Hash",
                                "uco-types:hashMethod": {
                                    "@type": "uco-vocabulary:HashNameVocab",
                                    "@value": "MD5"
                                },
                                "uco-types:hashValue": {
                                    "@type": "xsd:hexBinary",
                                    "@value": hashlib.md5(filepath).hexdigest()
                                }
                            },
                            {
                                "@type": "uco-types:Hash",
                                "uco-types:hashMethod": {
                                    "@type": "uco-vocabulary:HashNameVocab",
                                    "@value": "SHA1"
                                },
                                "uco-types:hashValue": {
                                    "@type": "xsd:hexBinary",
                                    "@value": hashlib.sha1(filepath).hexdigest()
                                }
                            }
                        ]
                    }
                ]
            })
        else:
            logging.critical('Invalid filepath')

    def generate_investigation_action(self):
        self.case['@graph'].append({
            "@id": ("kb:investigative-action" + str(uuid.uuid4())),
            "@type": "case-investigation:InvestigativeAction",
            "uco-action:startTime": {
                "@type": "xsd:dateTime",
                "@value": self.start_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            },
            "uco-action:endTime": {
                "@type": "xsd:dateTime",
                "@value": self.end_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            }
        })

    def generate_header(self):
        self.case['@context'] = {
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

    def generate_tool_signature(self):
        self.case['@graph'].append({
            "@id": ("kb:sqlite-dissect" + str(uuid.uuid4())),
            "@type": "uco-tool:Tool",
            "uco-core:name": "SQLite Dissect",
            "uco-tool:toolType": "Extraction",
            "uco-tool:creator": "Department of Defense Cyber Crime Center (DC3)",
            "uco-tool:version": __version__,
        })

    def export_case_file(self, export_path='output/case.json'):
        self.generate_header()
        self.generate_tool_signature()
        self.generate_investigation_action()

        with open(export_path, 'w') as f:
            json.dump(self.case, f, ensure_ascii=False, indent=4)
