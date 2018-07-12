package es.upm.fi.dia.oeg.mappingpedia

import java.io._
import java.net.{HttpURLConnection, URL}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.{Date, Properties, UUID}

import es.upm.fi.dia.oeg.mappingpedia.MappingPediaRunner.logger
import es.upm.fi.dia.oeg.mappingpedia.controller.{MappingDocumentController}
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.model.result.{GeneralResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.utility._
import org.apache.commons.cli.CommandLine
import org.apache.commons.lang.text.StrSubstitutor
import org.apache.jena.graph.Triple
import org.apache.jena.ontology.OntModel
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.impl.StatementImpl
import org.apache.jena.vocabulary.RDF
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.web.multipart.MultipartFile
import virtuoso.jena.driver.{VirtGraph, VirtModel, VirtuosoQueryExecutionFactory}

import scala.collection.JavaConversions._
import scala.io.Source.fromFile
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.eclipse.egit.github.core.client.GitHubClient

import scala.io.{BufferedSource, Source}






//class MappingPediaR2RML(mappingpediaGraph:VirtGraph) {
//class MappingPediaEngine() {

object MappingPediaEngine {
	val logger: Logger = LoggerFactory.getLogger(this.getClass);
	val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
	var ontologyModel:OntModel = null;
	var mappingpediaProperties:MappingPediaProperties = new MappingPediaProperties();
	var githubClient:GitHubUtility = null;
	var ckanClient:MpcCkanUtility = null;
	var virtuosoClient:VirtuosoClient = null;
	var jenaClient:JenaClient = null;
  val configurationFilename = "config.properties"

  def init(properties:MappingPediaProperties) = {
    this.mappingpediaProperties = properties;
    //val propertiesInString = this.mappingpediaProperties.mkString("\n");
    //logger.info(s"propertiesInString = ${propertiesInString}");

    this.githubClient = if (mappingpediaProperties.githubEnabled) {
      try {
        new GitHubUtility(mappingpediaProperties.githubRepository, mappingpediaProperties.githubUser
          , mappingpediaProperties.githubAccessToken)
      } catch {
        case e: Exception => {
          e.printStackTrace();
          null;
        }
      }
    } else { null }


    this.ckanClient = if(mappingpediaProperties.ckanEnable) {
      try {
        new MpcCkanUtility(mappingpediaProperties.ckanURL, mappingpediaProperties.ckanKey);
      } catch {
        case e: Exception =>
          e.printStackTrace()
          null
      }
    } else { null }

    this.virtuosoClient = if (mappingpediaProperties.virtuosoEnabled) {
      try {
        new VirtuosoClient(mappingpediaProperties.virtuosoJDBC
          , mappingpediaProperties.virtuosoUser, mappingpediaProperties.virtuosoPwd, mappingpediaProperties.graphName)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          null
      }
    } else { null }

    this.jenaClient = if (virtuosoClient != null) {
      try {
        val schemaOntology = JenaClient.loadSchemaOrgOntology(virtuosoClient
          , MappingPediaConstant.SCHEMA_ORG_FILE, MappingPediaConstant.FORMAT)
        this.setOntologyModel(schemaOntology)
        new JenaClient(schemaOntology)
      } catch {
        case e: Exception =>
          e.printStackTrace();
          null;
      }
    } else { null }
  }




	/*	def uploadNewMapping(mappingpediaUsername: String, manifestFileRef: MultipartFile, mappingFileRef: MultipartFile
                         , replaceMappingBaseURI: String, generateManifestFile:String
                         , mappingDocumentTitle: String, mappingDocumentCreator:String, mappingDocumentSubjects:String
                         //, datasetTitle:String, datasetKeywords:String, datasetPublisher:String, datasetLanguage:String
                        ): MappingPediaExecutionResult = {
      logger.debug("mappingpediaUsername = " + mappingpediaUsername)
      // Path where the uploaded files will be stored.
      val uuid = UUID.randomUUID.toString
      logger.debug("uuid = " + uuid)
      this.uploadNewMapping(mappingpediaUsername, uuid, manifestFileRef, mappingFileRef, replaceMappingBaseURI
        , generateManifestFile, mappingDocumentTitle, mappingDocumentCreator, mappingDocumentSubjects
        //, datasetTitle, datasetKeywords, datasetPublisher, datasetLanguage
      );
    }*/






  def generateStringFromTemplateFiles(map: Map[String, String]
                                     , templateFilesPaths:Iterable[String]) : String = {
    try {

      val templateLines:String = templateFilesPaths.map(templateFilePath => {
        val templateStream: InputStream = getClass.getResourceAsStream(
          "/" + templateFilePath);
        val templateLinesAux:String = scala.io.Source.fromInputStream(templateStream)
          .getLines.mkString("\n");
        templateLinesAux
      }).mkString("\n\n")

      val generatedLines = map.foldLeft(templateLines)( (acc, kv) => {
        val mapValue:String = map.get(kv._1).getOrElse("");
        if(mapValue ==null){
          logger.debug("the input value for " + kv._1 + " is null");
          acc.replaceAllLiterally(kv._1, "")
        } else {
          acc.replaceAllLiterally(kv._1, mapValue)
        }
      });

      generatedLines;
    } catch {
      case e:Exception => {
        logger.error("error generating file from template: " + templateFilesPaths);
        e.printStackTrace();
        throw e
      }
    }
  }

	def generateStringFromTemplateFile(map: Map[String, String]
																		 , templateFilePath:String) : String = {
		//logger.info(s"Generating string from template file: $templateFilePath ...")
		try {

			//var lines: String = Source.fromResource(templateFilePath).getLines.mkString("\n");
			val templateStream: InputStream = getClass.getResourceAsStream("/" + templateFilePath)
			val templateLines = scala.io.Source.fromInputStream(templateStream).getLines.mkString("\n");

			val generatedLines = map.foldLeft(templateLines)( (acc, kv) => {
				val mapValue:String = map.get(kv._1).getOrElse("");
				if(mapValue ==null){
					logger.debug("the input value for " + kv._1 + " is null");
					acc.replaceAllLiterally(kv._1, "")
				} else {
					acc.replaceAllLiterally(kv._1, mapValue)
				}
				//logger.info("replacing " + kv._1 + " with " + mapValue);

			});


			/*
			var lines3 = lines;
			map.keys.foreach(key => {
				lines3 = lines3.replaceAllLiterally(key, map(key));
      })
			logger.info("lines3 = " + lines3)
			*/

			//logger.info(s"String from template file $templateFilePath generated.")
			generatedLines;
		} catch {
			case e:Exception => {
				logger.error("error generating file from template: " + templateFilePath);
				e.printStackTrace();
				throw e
			}
		}
	}


	def generateManifestString(map: Map[String, String], templateFiles:List[String]) : String = {
		val manifestString = templateFiles.foldLeft("") { (z, i) => {
			//logger.info("templateFiles.foldLeft" + (z, i))
			z + this.generateStringFromTemplateFile(map, i) + "\n\n" ;
		} }

		manifestString;
	}

	def generateManifestFile(map: Map[String, String], templateFiles:List[String], filename:String, datasetID:String) : File = {
		val manifestString = this.generateManifestString(map, templateFiles);
		this.generateManifestFile(manifestString, filename, datasetID);
	}

	def generateManifestFile(manifestString:String, filename:String, datasetID:String) : File = {
		try {
			//def mappingDocumentLines = this.generateManifestLines(map, "templates/metadata-mappingdocument-template.ttl");
			//logger.debug("mappingDocumentLines = " + mappingDocumentLines)

			//def datasetLines = this.generateManifestLines(map, "templates/metadata-dataset-template.ttl");
			//logger.debug("datasetLines = " + datasetLines)

			val uploadDirectoryPath: String = MappingPediaConstant.DEFAULT_UPLOAD_DIRECTORY;
			val outputDirectory: File = new File(uploadDirectoryPath)
			if (!outputDirectory.exists) {
				outputDirectory.mkdirs
			}
			val uuidDirectoryPath: String = uploadDirectoryPath + "/" + datasetID
			//logger.info("upload directory path = " + uuidDirectoryPath)
			val uuidDirectory: File = new File(uuidDirectoryPath)
			if (!uuidDirectory.exists) {
				uuidDirectory.mkdirs
			}

			val file = new File(uuidDirectory + "/" + filename)
			val bw = new BufferedWriter(new FileWriter(file))
			//logger.info(s"manifestTriples = $manifestTriples")
			bw.write(manifestString)
			bw.close()
			file
		} catch {
			case e:Exception => {
				logger.error("error generating manifest file: " + e.getMessage);
				e.printStackTrace();
				null

			}
		}
	}






	def storeRDFFile(fileRef: MultipartFile, graphURI: String): GeneralResult = {
		try {
			val file = MappingPediaUtility.multipartFileToFile(fileRef)
			val filePath = file.getPath
			logger.info("file path = " + filePath)
			this.virtuosoClient.storeFromFilePath(filePath)
			val errorCode = HttpURLConnection.HTTP_CREATED
			val status = "success, file uploaded to: " + filePath
			logger.info("file inserted.")
			val executionResult = new GeneralResult(status, errorCode)
			executionResult
		} catch {
			case e: Exception =>
				val errorMessage = "error processing uploaded file: " + e.getMessage
				logger.error(errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val status = "failed, error message = " + e.getMessage
				val executionResult = new GeneralResult(status, errorCode)
				executionResult
		}
	}







	def getAllTriplesMaps() = {
		val prolog = "PREFIX rr: <http://www.w3.org/ns/r2rml#> \n"
		var queryString: String = prolog + "SELECT ?tm \n";
		queryString = queryString + " FROM <" + this.mappingpediaProperties.graphName + ">\n";
		queryString = queryString + " WHERE {?tm rr:logicalTable ?lt} \n";

		val m = VirtModel.openDatabaseModel(this.mappingpediaProperties.graphName, this.mappingpediaProperties.virtuosoJDBC
			, this.mappingpediaProperties.virtuosoUser, this.mappingpediaProperties.virtuosoPwd);

		logger.debug("Executing query=\n" + queryString)

		val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
		var results:List[String] = List.empty;
		try {
			val rs = qexec.execSelect
			while (rs.hasNext) {
				val rb = rs.nextSolution
				val rdfNode = rb.get("tm");
				val rdfNodeInString = rb.get("tm").toString;
				results = rdfNodeInString :: results;
			}
		} finally qexec.close

		val listResult = new ListResult[String](results.length, results);
		listResult
	}








	def getSubclassesSummary(pClass:String) = {
		//val classURI = MappingPediaUtility.getClassURI(pClass, "http://schema.org/");

		val normalizedClasses = MappingPediaUtility.normalizeTerm(pClass).distinct;
		logger.info(s"normalizedClasses = $normalizedClasses");

		val resultAux:List[String] = normalizedClasses.flatMap(normalizedClass => {
			logger.info(s"normalizedClass = $normalizedClass");
			val schemaClass:String = jenaClient.mapNormalizedTerms.getOrElse(normalizedClass, normalizedClass);
			logger.info(s"schemaClass = $schemaClass");
			jenaClient.getSubclassesSummary(schemaClass).results.asInstanceOf[List[String]]
		}).distinct;
		new ListResult(resultAux.size, resultAux)
	}

	def getSchemaOrgSubclassesDetail(aClass:String) = {
		logger.info(s"jenaClient = $jenaClient")
		logger.info(s"this.ontologyModel = ${this.ontologyModel}")

		jenaClient.getSubclassesDetail(aClass);
	}













	def addQueryFile(queryFileRef: MultipartFile, mappingpediaUsername:String, datasetID:String) : GeneralResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("datasetID = " + datasetID)

		try {
			val queryFile:File = MappingPediaUtility.multipartFileToFile(queryFileRef, datasetID)

			logger.info("storing a new query file in github ...")
			val commitMessage = "Add a new query file by mappingpedia-engine"
			val response = githubClient.encodeAndPutFile(mappingpediaUsername
				, datasetID, queryFile.getName, commitMessage, queryFile)
			logger.debug("response.getHeaders = " + response.getHeaders)
			logger.debug("response.getBody = " + response.getBody)
			val responseStatus = response.getStatus
			logger.debug("responseStatus = " + responseStatus)
			val responseStatusText = response.getStatusText
			logger.debug("responseStatusText = " + responseStatusText)
			if (HttpURLConnection.HTTP_CREATED == responseStatus) {
				val queryURL = response.getBody.getObject.getJSONObject("content").getString("url")
				logger.debug("queryURL = " + queryURL)
				logger.info("dataset stored.")
				val executionResult = new GeneralResult(responseStatusText, responseStatus)
				return executionResult
			}
			else {
				val executionResult = new GeneralResult(responseStatusText, responseStatus)
				return executionResult
			}
		} catch {
			case e: Exception =>
				val errorMessage = e.getMessage
				logger.error("error uploading a new query file: " + errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val executionResult = new GeneralResult(errorMessage, errorCode)
				return executionResult
		}
	}


	def getMapping(mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String):GeneralResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("mappingDirectory = " + mappingDirectory)
		logger.debug("mappingFilename = " + mappingFilename)
		val response = githubClient.getFile(
			//MappingPediaEngine.mappingpediaProperties.githubUser
			//MappingPediaEngine.mappingpediaProperties.githubAccessToken,
			mappingpediaUsername, mappingDirectory, mappingFilename)
		val responseStatus = response.getStatus
		logger.debug("responseStatus = " + responseStatus)
		val responseStatusText = response.getStatusText
		logger.debug("responseStatusText = " + responseStatusText)
		val executionResult = if (HttpURLConnection.HTTP_OK == responseStatus) {
			val githubMappingURL = response.getBody.getObject.getString("url")
			logger.debug("githubMappingURL = " + githubMappingURL)
			new GeneralResult(responseStatusText, responseStatus)
		} else {
			new GeneralResult(responseStatusText, responseStatus)
		}
		executionResult;
	}

  /*
	def updateExistingMapping(mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String
														, mappingFileRef:MultipartFile): GeneralResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("mappingDirectory = " + mappingDirectory)
		logger.debug("mappingFilename = " + mappingFilename)
		logger.debug("mappingFileRef = " + mappingFileRef)
		try {
			val mappingFile = MappingPediaUtility.multipartFileToFile(mappingFileRef, mappingDirectory)
			val mappingFilePath = mappingFile.getPath
			logger.debug("mapping file path = " + mappingFilePath)
			val commitMessage = "Mapping modification by mappingpedia-engine.Application"
			val mappingContent = this.getMappingContent(null, null, mappingFilePath, null)
			val base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent)
			val response = githubClient.putEncodedContent(mappingpediaUsername, mappingDirectory, mappingFilename
				, commitMessage, base64EncodedContent)
			val responseStatus = response.getStatus
			logger.debug("responseStatus = " + responseStatus)
			val responseStatusText = response.getStatusText
			logger.debug("responseStatusText = " + responseStatusText)

			val executionResult = if (HttpURLConnection.HTTP_OK == responseStatus) {
				val githubMappingURL = response.getBody.getObject.getJSONObject("content").getString("url")
				logger.debug("githubMappingURL = " + githubMappingURL)
				new GeneralResult(responseStatusText, responseStatus)
			} else {
				new GeneralResult(responseStatusText, responseStatus)
			}
			executionResult;
		} catch {
			case e: Exception =>
				e.printStackTrace()
				val errorMessage = "error processing the uploaded mapping file: " + e.getMessage
				logger.error(errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val executionResult = new GeneralResult(errorMessage, errorCode)
				executionResult
		}
	}
	*/

	def setOntologyModel(ontModel: OntModel) = { this.ontologyModel = ontModel }

	def setProperties(properties: MappingPediaProperties) = { this.mappingpediaProperties = properties }




}