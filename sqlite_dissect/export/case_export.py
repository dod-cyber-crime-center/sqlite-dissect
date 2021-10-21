import hashlib
import json
import uuid
from datetime import datetime
from os import path

from _version import __version__

"""
This script holds the objects used for exporting information regarding SQLite carving process to CASE format. 
Information about the CASE Cyber Ontology can be found at: https://caseontology.org/
"""


class CaseExporter(object):
    # Define the formatted logger that is provided by the main.py execution path
    logger = None

    # Defines the initial structure for the CASE export. This will be supplemented with various methods that get called
    # from the main.py execution path.
    case = {
        '@context': {
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
        },
        '@graph': [
            {
                "@id": ("kb:sqlite-dissect" + str(uuid.uuid4())),
                "@type": "uco-tool:Tool",
                "uco-core:name": "SQLite Dissect",
                "uco-tool:toolType": "Extraction",
                "uco-tool:creator": "Department of Defense Cyber Crime Center (DC3)",
                "uco-tool:version": __version__,
            }
        ]
    }
    start_datetime = None
    end_datetime = None

    def __init__(self, logger):
        self.logger = logger

    def register_options(self, options):
        """
        Adds the command line options provided as the configuration values provided and outputting them in the schema
        defined in the uco-tool namespace.

        Ontology Source: https://github.com/ucoProject/UCO/blob/master/uco-tool/tool.ttl
        """
        configuration_options = []

        # Loop through the list of provided options and add each configuration option to the CASE output
        for option in vars(options):
            configuration_options.append({
                "@type": "uco-tool:ConfigurationSettingType",
                "uco-tool:itemName": option,
                "uco-tool:itemValue": getattr(options, option)
            })

        # Build the configuration wrapper which includes the facet for the configuration
        configuration = [
            {
                "@type": "uco-tool:ToolConfigurationTypeFacet",
                "uco-tool:configurationSettings": configuration_options
            }
        ]

        # Add the configuration facet to the in progress CASE object
        self.case['@graph'][0]['uco-core:hasFacet'] = configuration


    def add_observable_file(self, filepath):
        """
        Adds the file specified in the provided filepath as an ObservableObject in the CASE export. This method handles
        calculation of filesize, extension, MD5 hash, SHA1 hash, and other metadata expected in the Observable TTL spec.

        Ontology source: https://github.com/ucoProject/UCO/blob/master/uco-observable/observable.ttl.
        """
        if path.exists(filepath) and path.isfile(filepath):
            # Get the full path we need for reference
            filepath = path.abspath(filepath)

            # Since the extension may take some additional logic checks, compute it out of the main JSON block
            extension = path.splitext(filepath)[1]
            if len(extension) > 0:
                extension = extension[1:]

            # Parse the file and get the attributes we need
            self.case['@graph'].append({
                "@id": ("kb:" + str(uuid.uuid4())),
                "@type": "uco-observable:ObservableObject",
                "uco-observable:hasChanged": False,
                "uco-core:hasFacet": [
                    {
                        "@type": "uco-observable:FileFacet",
                        "uco-observable:observableCreatedTime": {
                            "@type": "xsd:dateTime",
                            "@value": datetime.fromtimestamp(path.getctime(filepath)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        },
                        "uco-observable:modifiedTime": {
                            "@type": "xsd:dateTime",
                            "@value": datetime.fromtimestamp(path.getmtime(filepath)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        },
                        "uco-observable:accessedTime": {
                            "@type": "xsd:dateTime",
                            "@value": datetime.fromtimestamp(path.getatime(filepath)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        },
                        "uco-observable:extension": extension,
                        "uco-observable:fileName": path.basename(filepath),
                        "uco-observable:filePath": filepath,
                        "uco-observable:isDirectory": False,
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
                            },
                            {
                                "@type": "uco-types:Hash",
                                "uco-types:hashMethod": {
                                    "@type": "uco-vocabulary:HashNameVocab",
                                    "@value": "SHA256"
                                },
                                "uco-types:hashValue": {
                                    "@type": "xsd:hexBinary",
                                    "@value": hashlib.sha256(filepath).hexdigest()
                                }
                            },
                            {
                                "@type": "uco-types:Hash",
                                "uco-types:hashMethod": {
                                    "@type": "uco-vocabulary:HashNameVocab",
                                    "@value": "SHA512"
                                },
                                "uco-types:hashValue": {
                                    "@type": "xsd:hexBinary",
                                    "@value": hashlib.sha512(filepath).hexdigest()
                                }
                            }
                        ]
                    }
                ]
            })
        else:
            self.logger.critical('Attempting to add invalid filepath to CASE Observable export: {}'.format(filepath))

    def generate_investigation_action(self):
        """
        Builds the investigative action object as defined in the CASE ontology. This also takes in the start and end
        datetimes from the analysis.

        Ontology source: https://github.com/casework/CASE/blob/master/ontology/investigation/investigation.ttl
        """
        action = {
            "@id": ("kb:investigative-action" + str(uuid.uuid4())),
            "@type": "case-investigation:InvestigativeAction"
        }

        if self.start_datetime:
            action["uco-action:startTime"] = {
                "@type": "xsd:dateTime",
                "@value": self.start_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            }
        if self.end_datetime:
            action["uco-action:endTime"] = {
                "@type": "xsd:dateTime",
                "@value": self.end_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            }

        self.case['@graph'].append(action)

    def export_case_file(self, export_path='output/case.json'):
        """
        Exports the built CASE object to the path specified in the export_path parameter.
        """

        # Write the CASE export to the filesystem
        with open(export_path, 'w') as f:
            json.dump(self.case, f, ensure_ascii=False, indent=4)
            self.logger.info('CASE formatted file has been exported to {}'.format(export_path))
