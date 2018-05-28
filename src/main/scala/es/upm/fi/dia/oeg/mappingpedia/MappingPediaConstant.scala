package es.upm.fi.dia.oeg.mappingpedia

import java.text.SimpleDateFormat

import org.apache.jena.rdf.model.ResourceFactory


/**
  * Created by freddy on 10/08/2017.
  */
object MappingPediaConstant {
	val MAPPINGPEDIA_ENGINE_VERSION = "0.9.0";

  //RDB2RDF Test related constants
	//private val RDB2RDFTEST_NS = "http://purl.org/NET/rdb2rdf-test#";
	//private val RDB2RDFTEST_R2RML_URI = RDB2RDFTEST_NS + "R2RML";
	//val RDB2RDFTEST_R2RML_CLASS = ResourceFactory.createResource(RDB2RDFTEST_R2RML_URI);
	//private val RDB2RDFTEST_MAPPING_DOCUMENT_URI = RDB2RDFTEST_NS + "mappingDocument";
	//val RDB2RDFTEST_MAPPING_DOCUMENT_PROPERTY = ResourceFactory.createProperty(RDB2RDFTEST_MAPPING_DOCUMENT_URI);

	//W3C Test Description related constants
	//private val TEST_NS = "http://www.w3.org/2006/03/test-description#";
	//private val TEST_PURPOSE_URI = TEST_NS + "purpose";
	//val TEST_PURPOSE_PROPERTY = ResourceFactory.createProperty(TEST_PURPOSE_URI);

	//R2RML related constants
	val R2RML_NS = "http://www.w3.org/ns/r2rml#";
	val R2RML_TRIPLESMAP_URI = R2RML_NS + "TriplesMap";
	val R2RML_TRIPLESMAP_CLASS = ResourceFactory.createResource(R2RML_TRIPLESMAP_URI);
	val R2RML_LOGICALTABLE_URI = R2RML_NS + "logicalTable";
	val R2RML_LOGICALTABLE_PROPERTY = ResourceFactory.createProperty(R2RML_LOGICALTABLE_URI);

	//RML related constants
	val RML_NS = "http://semweb.mmlab.be/ns/rml#";
	val RML_LOGICALSOURCE_URI = RML_NS + "logicalSource";
	val RML_LOGICALSOURCE_PROPERTY = ResourceFactory.createProperty(RML_LOGICALSOURCE_URI);

	//Mappingpedia related constants
	private val MAPPINGPEDIA_NS = "http://mappingpedia.linkeddata.es/vocabulary#";
	val MAPPINGPEDIA_INSTANCE_NS = "http://mappingpedia.linkeddata.es/instance/";
	val DEFAULT_MAPPINGPEDIA_GRAPH = "http://mappingpedia.linkeddata.es/graph/data";
	private val HAS_TRIPLES_MAPS_URI = MAPPINGPEDIA_NS + "hasTriplesMaps";
	val HAS_TRIPLES_MAPS_PROPERTY = ResourceFactory.createProperty(HAS_TRIPLES_MAPS_URI);
	private val DEFAULT_MAPPINGDOCUMENTFILE_URI = "http://mappingpedia.linkeddata.es/vocabulary#hasMappingDocumentFile";
	val DEFAULT_MAPPINGDOCUMENTFILE_PROPERTY = ResourceFactory.createProperty(DEFAULT_MAPPINGDOCUMENTFILE_URI);
  val MAPPINGPEDIAVOCAB_R2RMLMAPPINGDOCUMENT_URI = MAPPINGPEDIA_NS + "R2RMLMappingDocument";
	val MAPPINGPEDIAVOCAB_R2RMLMAPPINGDOCUMENT_CLASS = ResourceFactory.createResource(MAPPINGPEDIAVOCAB_R2RMLMAPPINGDOCUMENT_URI);
	val MAPPINGPEDIAVOCAB_RMLMAPPINGDOCUMENT_URI = MAPPINGPEDIA_NS + "RMLMappingDocument";
	val MAPPINGPEDIAVOCAB_RMLMAPPINGDOCUMENT_CLASS = ResourceFactory.createResource(MAPPINGPEDIAVOCAB_RMLMAPPINGDOCUMENT_URI);
	val MAPPINGPEDIAVOCAB_MAPPINGDOCUMENT_URI = MAPPINGPEDIA_NS + "MappingDocument";
	val MAPPINGPEDIAVOCAB_MAPPINGDOCUMENT_CLASS = ResourceFactory.createResource(MAPPINGPEDIAVOCAB_MAPPINGDOCUMENT_URI);

	val MANIFEST_FILE_LANGUAGE = "TURTLE";
	val R2RML_FILE_LANGUAGE = "TURTLE";
	val DEFAULT_OUTPUT_FILENAME = "output.nt";

	val DEFAULT_UPLOAD_DIRECTORY = "upload-dir";

	//val DEFAULT_GITHUB_REPOSITORY="https://github.com/oeg-upm/mappingpedia-contents";
	//val DEFAULT_GITHUB_REPOSITORY_CONTENTS="https://api.github.com/repos/oeg-upm/mappingpedia-contents";

	val SEARCH_MAPPINGDOCUMENT_BY_CLASS="1";
	val SEARCH_MAPPINGDOCUMENT_BY_PROPERTY="2";
	val SEARCH_MAPPINGDOCUMENT_BY_TABLE="4";
	val SEARCH_MAPPINGDOCUMENT_BY_COLUMN="8";

	val MAPPING_LANGUAGE_R2RML = "r2rml";
	val MAPPING_LANGUAGE_RML = "rml";
	val MAPPING_LANGUAGE_xR2RML = "xr2rml";

	val TEMPLATE_MAPPINGDOCUMENT_METADATA = "templates/metadata-mappingdocument-template.ttl";
	val TEMPLATE_MAPPINGDOCUMENT_METADATA_NAMESPACE = "templates/metadata-namespaces-template.ttl";

	val GITHUB_RAW_URL_PREFIX = "https://raw.githubusercontent.com/";
	val GITHUB_ACCESS_URL_PREFIX = "https://api.github.com/repos/";

	val SCHEMA_ORG_FILE = "schema.rdf"
	//val SCHEMA_ORG_FILE = "tree.jsonld"
	val FORMAT = "RDF/XML"

	val SCHEMA_RANGE_INCLUDES_PROPERTY = ResourceFactory.createProperty("http://schema.org/rangeIncludes");
	val SCHEMA_DOMAIN_INCLUDES_PROPERTY = ResourceFactory.createProperty("http://schema.org/domainIncludes");


	// CUSTOM CKAN FIELDS
	val CKAN_RESOURCE_ORIGINAL_DATASET_DISTRIBUTION_DOWNLOAD_URL = "original_dataset_distribution_download_url";
	val CKAN_RESOURCE_MAPPING_DOCUMENT_DOWNLOAD_URL = "mapping_document_download_url";
	val CKAN_RESOURCE_PROV_TRIPLES = "prov_triples";
	val CKAN_RESOURCE_CLASS = "class";
	val CKAN_RESOURCE_CLASSES = "tag_string";
	val CKAN_RESOURCE_IS_ANNOTATED = "is_annotated_data";
  val CKAN_RESOURCE_ORIGINAL_DISTRIBUTION_CKAN_ID = "original_resource_id";


	val SDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	val DEFAULT_CONFIGURATION_FILENAME = "config.properties";
}
