package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.util.{Date, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.{githubClient, logger, virtuosoClient}
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingDocumentController.logger
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.model.result.{AddMappingDocumentResult, GeneralResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.utility._
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.impl.StatementImpl
import org.apache.jena.vocabulary.RDF
import org.springframework.web.multipart.MultipartFile

import scala.collection.JavaConversions._
import scala.io.Source
import scala.io.Source.fromFile

class MappingDocumentController(
                                 ckanUtility:MpcCkanUtility
                                 , githubUtility:MpcGithubUtility
                                 , virtuosoUtility:MpcVirtuosoUtility
                                 , jenaUtility:MPCJenaUtility
                               ) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);







  val mapper = new ObjectMapper();

  def findOrCreate(id:String): MappingDocument = {
    val existingMappingDocument:MappingDocument = if(id != null) {
      this.findById(id).results.iterator.next();
    } else { null }
    val mappingDocument = if(existingMappingDocument == null) {
      new MappingDocument();
    } else { existingMappingDocument }
    mappingDocument
  }

  def storeMappingDocumentOnGitHub(mappingDocument:MappingDocument, organizationId:String, datasetId:String) = {

    val mappingDocumentDownloadURL = mappingDocument.getDownloadURL();

    val (mappingDocumentFileName:String, mappingDocumentFileContent:String) =
      MappingPediaUtility.getFileNameAndContent(mappingDocument.mappingDocumentFile, mappingDocumentDownloadURL, "UTF-8");
    val base64EncodedContent = GitHubUtility.encodeToBase64(mappingDocumentFileContent)

    //val mappingDocumentFilePath = s"${organization.dctIdentifier}/${dataset.dctIdentifier}/${mappingDocument.dctIdentifier}/${mappingDocumentFileName}";
    val mappingDocumentFilePath = s"${organizationId}/${datasetId}/${mappingDocumentFileName}";
    logger.info(s"mappingDocumentFilePath = $mappingDocumentFilePath")

    val commitMessage = s"add mapping document file ${mappingDocumentFileName}"
    //val mappingContent = MappingPediaEngine.getMappingContent(mappingFilePath)

    logger.info("STORING MAPPING DOCUMENT FILE ON GITHUB ...")
    val response = githubUtility.putEncodedContent(
      mappingDocumentFilePath, commitMessage, base64EncodedContent)
    val responseStatus = response.getStatus
    if (HttpURLConnection.HTTP_OK == responseStatus
      || HttpURLConnection.HTTP_CREATED == responseStatus) {
      val githubDownloadURL = response.getBody.getObject.getJSONObject("content").getString("download_url");
      mappingDocument.setDownloadURL(githubDownloadURL);
      logger.info("Mapping stored on GitHub")
    } else {
      val errorMessage = "Error when storing mapping on GitHub: " + responseStatus
      throw new Exception(errorMessage);
    }
    response
  }


  def addNewMappingDocument(
                             organizationId:String
                             , datasetId: String
                             , datasetPackageId:String
                             //, manifestFileRef: MultipartFile
                             , pManifestFile: File
                             , replaceMappingBaseURI: String
                             , generateManifestFile: Boolean
                             , mappingDocument: MappingDocument
                           ): AddMappingDocumentResult = {
    var errorOccured = false;
    var collectiveErrorMessage: List[String] = Nil;


    val ckanPackageId = if(datasetPackageId == null) {
      try {
        val foundPackages = this.ckanUtility.findPackageByPackageName(organizationId
          , datasetId);

        if(foundPackages.size > 0) {
          foundPackages.head.getString("id")
        } else { null }
      } catch {
        case e:Exception => null
      }
    } else { datasetPackageId }
    logger.info("ckanPackageId = " + ckanPackageId);


    val mappingDocumentFile = mappingDocument.mappingDocumentFile;
    //val mappingFilePath = mappingFile.getPath

    //STORING MAPPING DOCUMENT FILE ON GITHUB
    val mappingFileGitHubResponse: HttpResponse[JsonNode] = try {
      this.storeMappingDocumentOnGitHub(mappingDocument, organizationId, datasetId);
    } catch {
      case e: Exception =>
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error generating manifest file: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
    }
    mappingDocument.accessURL = githubUtility.getAccessURL(mappingFileGitHubResponse)
    mappingDocument.setDownloadURL(githubUtility.getDownloadURL(mappingDocument.accessURL))
    val mappingDocumentDownloadURL = mappingDocument.getDownloadURL();
    mappingDocument.hash = MappingPediaUtility.calculateHash(mappingDocumentDownloadURL).toString;


    //MANIFEST FILE
    /*
    val manifestFile = try {
      if (manifestFileRef != null) {
        logger.info("Manifest file is provided")
        MappingPediaUtility.multipartFileToFile(manifestFileRef, datasetId)
      } else {
        logger.info("Manifest file is not provided")
        if (generateManifestFile) {
          //GENERATE MANIFEST FILE IF NOT PROVIDED
          MappingDocumentController.generateManifestFile(mappingDocument, datasetId, datasetPackageId);
        } else {
          null
        }
      }
    }
    catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace();
        val errorMessage = "Error occured when generating manifest file: " + e.getMessage;
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }
    */

    //MANIFEST FILE
    val manifestFile = if(pManifestFile == null) {
      if (generateManifestFile) {
        //GENERATE MANIFEST FILE IF NOT PROVIDED
        MappingDocumentController.generateManifestFile(mappingDocument, datasetId, datasetPackageId);
      } else {
        null
      }
    } else {
      pManifestFile
    }

    //STORING MAPPING AND MANIFEST FILES ON VIRTUOSO
    val virtuosoStoreMappingStatus = if(MappingPediaEngine.mappingpediaProperties.virtuosoEnabled) {
      try {
        logger.info("STORING MAPPING DOCUMENT AND ITS MANIFEST ON VIRTUOSO ...")
        val manifestFilePath: String = if (manifestFile == null) {
          null
        } else {
          manifestFile.getPath;
        }
        val newMappingBaseURI = MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS + mappingDocument.dctIdentifier + "/"
        val clearGraph = "false";
        this.storeManifestAndMapping(mappingDocument.mappingLanguage
          , manifestFilePath, mappingDocument.getDownloadURL(), clearGraph
          , replaceMappingBaseURI, newMappingBaseURI)
        logger.info("Mapping and manifest file stored on Virtuoso")
        "OK"
      } catch {
        case e: Exception => {
          errorOccured = true;
          e.printStackTrace();
          val errorMessage = "Error occurred when storing mapping and manifest files on virtuoso: " + e.getMessage;
          logger.error(errorMessage)
          collectiveErrorMessage = errorMessage :: collectiveErrorMessage
          e.getMessage
        }
      }
    } else {
      "Storing to Virtuoso is not enabled!";
    }



    //STORING MANIFEST FILE ON GITHUB
    val addNewManifestResponse = try {
      if (manifestFile != null) {
        logger.info("STORING MANIFEST FILE ON GITHUB ...")
        val addNewManifestCommitMessage = s"Add mapping document manifest: ${mappingDocument.dctIdentifier}"

        val mappingDocumentManifestFilePath = s"${organizationId}/${datasetId}/${mappingDocument.dctIdentifier}/${manifestFile.getName}";
        logger.info(s"mappingDocumentManifestFilePath = $mappingDocumentManifestFilePath")

        val githubResponse = githubUtility.encodeAndPutFile(
          mappingDocumentManifestFilePath, addNewManifestCommitMessage, manifestFile)
        val addNewManifestResponseStatus = githubResponse.getStatus
        val addNewManifestResponseStatusText = githubResponse.getStatusText

        if (HttpURLConnection.HTTP_CREATED == addNewManifestResponseStatus
          || HttpURLConnection.HTTP_OK == addNewManifestResponseStatus) {
          logger.info("Manifest file stored on GitHub")
        } else {
          errorOccured = true;
          val errorMessage = "Error occured when storing manifest file on GitHub: " + addNewManifestResponseStatusText;
          logger.error(errorMessage)
          collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        }
        githubResponse
      } else {
        null
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace();
        val errorMessage = "Error occurred when storing manifest files on github: " + e.getMessage;
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }
    mappingDocument.manifestAccessURL = if (addNewManifestResponse == null) {
      null
    } else {
      addNewManifestResponse.getBody.getObject.getJSONObject("content").getString("url")
    }
    mappingDocument.manifestDownloadURL = githubUtility.getDownloadURL(mappingDocument.manifestAccessURL);

    val (responseStatus, responseStatusText) = if (errorOccured) {
      (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
    } else {
      (HttpURLConnection.HTTP_OK, "OK")
    }


    val addMappingResult:AddMappingDocumentResult = new AddMappingDocumentResult(
      responseStatus, responseStatusText
      //, mappingDocumentAccessURL, mappingDocumentDownloadURL, mappingDocument.sha
      , mappingDocument
      //, manifestAccessURL, manifestDownloadURL
      , virtuosoStoreMappingStatus, virtuosoStoreMappingStatus
    )

    try {
      val addMappingDocumentResultAsJson = this.mapper.writeValueAsString(addMappingResult);
      logger.info(s"addMappingDocumentResultAsJson = ${addMappingDocumentResultAsJson}\n\n");
    } catch {
      case e:Exception => {
        logger.error(s"addMappingResult = ${addMappingResult}")
      }
    }

    addMappingResult

    /*
    new MappingPediaExecutionResult(manifestGitHubURL, null, mappingDocumentGitHubURL
      , null, null, responseStatusText, responseStatus, null)
      */


  }



  def findAllMappedClasses(): ListResult[String] = {
    this.findAllMappedClasses("http://schema.org")
  }

  def findAllMappedClasses(prefix:String): ListResult[String] = {

    //val queryString: String = MappingPediaUtility.readFromResourcesDirectory("templates/findAllMappingDocuments.rq")
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName,
      "$prefix" -> prefix
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findAllMappedClasses.rq")

    //logger.info(s"queryString = $queryString");
    /*
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);
    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    */
    val qexec = virtuosoUtility.createQueryExecution(queryString);

    var results: List[String] = List.empty;
    try {
      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val mappedClass = qs.get("mappedClass").toString;
        results = mappedClass :: results;
      }
    } finally qexec.close

    val listResult = new ListResult[String](results.length, results);
    listResult
  }

  def findMappedClassesByMappingDocumentId(mappingDocumentId:String) = {
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName,
      "$mappingDocumentId" -> mappingDocumentId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findMappedClassesByMappingDocumentId.rq")

    val qexec = virtuosoUtility.createQueryExecution(queryString);
    logger.debug(s"queryString = \n$queryString")

    var results: List[String] = List.empty;
    try {
      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val mappedClass = qs.get("mappedClass").toString;
        logger.info(s"mappedClass = $mappedClass")

        results = mappedClass :: results;
      }
    }
    catch {
      case e:Exception => { e.printStackTrace()}
    }
    finally qexec.close

    val listResult = new ListResult(results.length, results);
    listResult
  }

  def findAllMappedClassesByTableName(tableName:String): ListResult[String] = {
    this.findAllMappedClassesByTableName("http://schema.org", tableName)
  }

  def findAllMappedClassesByTableName(prefix:String, tableName:String): ListResult[String] = {

    //val queryString: String = MappingPediaUtility.readFromResourcesDirectory("templates/findAllMappingDocuments.rq")
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName,
      "$tableName" -> tableName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findMappedClassesByMappedTable.rq")

    //logger.info(s"queryString = $queryString");
    /*
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);
    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    */
    val qexec = virtuosoUtility.createQueryExecution(queryString);
    logger.debug(s"queryString = \n$queryString")

    var results: List[String] = List.empty;
    try {
      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val mappedClass = qs.get("mappedClass").toString;
        val count = qs.get("count").toString;
        logger.info(s"mappedClass = $mappedClass")
        logger.info(s"count = $count")

        results = s"$mappedClass -- $count" :: results;
      }
    }
    catch {
      case e:Exception => { e.printStackTrace()}
    }
    finally qexec.close

    val listResult = new ListResult[String](results.length, results);
    listResult
  }

  def findAllMappedProperties(prefix:String) = {
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName,
      "$prefix" -> prefix
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findAllMappedProperties.rq")

    //logger.info(s"queryString = $queryString");
    /*
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);
    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    */
    val qexec = virtuosoUtility.createQueryExecution(queryString);


    var results: List[String] = List.empty;
    try {
      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val mappedProperty = qs.get("mappedProperty").toString;
        results = mappedProperty :: results;
      }
    } finally qexec.close

    val listResult = new ListResult(results.length, results);
    listResult
  }


  def findAll() = {

    //val queryString: String = MappingPediaUtility.readFromResourcesDirectory("templates/findAllMappingDocuments.rq")
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findAllMappingDocuments.rq")
    logger.info(s"queryString = ${queryString}")

    this.findByQueryString(queryString);

  }

  def findByTerm(searchType: String, searchTerm: String): ListResult[MappingDocument] = {
    val result = if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_CLASS.equals(searchType) && searchTerm != null) {

      val listResult = this.findByClass(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_PROPERTY.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedProperty:" + searchTerm)
      val listResult = this.findByProperty(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_TABLE.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedTable:" + searchTerm)
      val listResult = this.findByTable(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_COLUMN.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedColumn:" + searchTerm)
      val listResult = this.findByColumn(searchTerm)
      listResult
    } else {
      logger.info("findAllMappingDocuments")
      val listResult = this.findAll
      listResult
    }
    //logger.info("result = " + result)

    result;
  }

  def findByDatasetId(pDatasetId: String, pCKANPackageId:String
                      , pCKANPackageName:String) = {

    val queryTemplateFile = "templates/findMappingDocumentsByDatasetId.rq";

    val datasetId = if(pDatasetId != null) {
      pDatasetId
    } else { "" }

    val ckanPackageId = if(pCKANPackageId != null) {
      pCKANPackageId
    } else { "" }

    val ckanPackageName = if(pCKANPackageName != null) {
      pCKANPackageName
    } else { "" }

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$datasetId" -> datasetId
      , "$ckanPackageId" -> ckanPackageId
      , "$ckanPackageName" -> ckanPackageName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    logger.debug(s"queryString = ${queryString}")
    this.findByQueryString(queryString);
  }

  def findByCKANPackageId(pCKANPackageId:String) = {

    val queryTemplateFile = "templates/findMappingDocumentsByCKANPackageId.rq";

    val ckanPackageId = if(pCKANPackageId != null) {
      pCKANPackageId
    } else { "" }

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$ckanPackageId" -> pCKANPackageId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    logger.debug(s"queryString = ${queryString}")
    this.findByQueryString(queryString);
  }

  def findByClass(mappedClass: String) = {
    //logger.info("findMappingDocumentsByMappedClass:" + mappedClass)
    val queryTemplateFile = "templates/findTriplesMapsByMappedClass.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedClass" -> mappedClass
      //, "$mappedProperty" -> mappedProperty
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findByClassAndProperty(mappedClass: String, mappedProperty:String): ListResult[MappingDocument] = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedClassAndProperty.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedClass" -> mappedClass
      , "$mappedProperty" -> mappedProperty
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findByClassAndProperty(aClass: String, aProperty:String, subclass: Boolean): ListResult[MappingDocument] = {
    val classURI = MappingPediaUtility.getClassURI(aClass);

    val normalizedClassURI = this.jenaUtility.mapNormalizedTerms.getOrElse(classURI, classURI);

    //val subclassesURIs:List[String] = jenaClient.getSubclassesSummary(normalizedClassURI).results.asInstanceOf[List[String]];
    val subclassesURIs:List[String] = if(subclass) {
      jenaUtility.getSubclassesSummary(normalizedClassURI).results.toList;
    } else {
      List(normalizedClassURI)
    }

    val allMappedClasses:List[String] = this.findAllMappedClasses().results.toList

    val intersectedClasses = subclassesURIs.intersect(allMappedClasses);

    val mappingDocuments = intersectedClasses.flatMap(intersectedClass => {
      this.findByClassAndProperty(intersectedClass, aProperty).getResults();
    });

    val listResult = new ListResult[MappingDocument](mappingDocuments.size, mappingDocuments)
    listResult
  }

  def findByDistributionId(distributionId: String) = {
    logger.info("findMappingDocumentsByDistributionId:" + distributionId)
    val queryTemplateFile = "templates/findMappingDocumentsByDistributionId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$distributionId" -> distributionId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findById(mappingDocumentId: String): ListResult[MappingDocument] = {
    logger.info("findMappingDocumentsByMappingDocumentId:" + mappingDocumentId)
    val queryTemplateFile = "templates/findMappingDocumentsByMappingDocumentId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappingDocumentId" -> mappingDocumentId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    logger.info(s"queryString = ${queryString}")

    val resultAux = this.findByQueryString(queryString).getResults();
    /*
    val result = if(resultAux != null && resultAux.iterator.size > 0) {
      resultAux.iterator().next()
    } else {
      null
    }
    */

    val result = new ListResult[MappingDocument](resultAux.size, resultAux);
    result
  }

  def findByProperty(mappedProperty: String) = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedProperty.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedProperty" -> mappedProperty
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findByColumn(mappedColumn: String) = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedColumn.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedColumn" -> mappedColumn
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findByTable(mappedTable: String) = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedTable.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedTable" -> mappedTable
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findByQueryString(queryString: String): ListResult[MappingDocument] = {
    //logger.info(s"queryString = $queryString");
    /*
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);
    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    */
    val qexec = virtuosoUtility.createQueryExecution(queryString);
    //logger.info(s"queryString = \n$queryString")

    var results: List[MappingDocument] = List.empty;
    try {
      var retrievedMappings:List[String] = Nil;

      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {

        val qs = rs.nextSolution
        val mdID= qs.get("mdID").toString;

        val md = new MappingDocument(mdID);
        md.dctTitle = MappingPediaUtility.getStringOrElse(qs, "title", null);

        md.datasetId = MappingPediaUtility.getStringOrElse(qs, "datasetId", null);
        logger.info(s"md.datasetId = ${md.datasetId}")
        val datasetModified = MappingPediaUtility.getStringOrElse(qs, "datasetModified", null);
        val datasetTitle = MappingPediaUtility.getStringOrElse(qs, "datasetTitle", null);

        val distributionAccessURL= MappingPediaUtility.getStringOrElse(qs, "distributionAccessURL", null);
        val distributionDownloadURL= MappingPediaUtility.getStringOrElse(qs, "distributionDownloadURL", null);
        val distributionHash = MappingPediaUtility.getStringOrElse(qs, "distributionHash", null);


        //md.dataset = MappingPediaUtility.getStringOrElse(qs, "dataset", null);
        //md.filePath = MappingPediaUtility.getStringOrElse(qs, "mappingDocumentFile", null);
        md.dctCreator = MappingPediaUtility.getStringOrElse(qs, "creator", null);

        md.mappingLanguage = MappingPediaUtility.getStringOrElse(qs, "mappingLanguage", null);

        md.dctDateSubmitted = MappingPediaUtility.getStringOrElse(qs, "dateSubmitted", null);

        md.hash = MappingPediaUtility.getStringOrElse(qs, "mdHash", null);

        md.ckanPackageId = MappingPediaUtility.getStringOrElse(qs, "packageId", null);

        md.ckanResourceId = MappingPediaUtility.getStringOrElse(qs, "resourceId", null);

        md.setDownloadURL(MappingPediaUtility.getStringOrElse(qs, "mdDownloadURL", null));
        //logger.info(s"md.sha = ${md.sha}");

        md.isOutdated = MappingDocumentController.isOutdatedMappingDocument(md.dctDateSubmitted, datasetModified);

        if(!retrievedMappings.contains(md.hash)) {
          //logger.warn(s"retrieving mapping document with sha ${md.sha}.")
          //results = md :: results;
          results = results :+ md

          retrievedMappings = retrievedMappings :+ md.hash
        } else {
          //logger.warn(s"mapping document with sha ${md.sha} has been retrived.")
        }

      }
    } finally qexec.close

    logger.debug(s"results.length = ${results.length}")

    val listResult = new ListResult[MappingDocument](results.length, results);
    listResult
  }



  def storeManifestAndMapping(mappingLanguage:String, manifestFilePath:String, pMappingFilePath:String
                              , clearGraphString:String, pReplaceMappingBaseURI:String, newMappingBaseURI:String
                             ): Unit = {

    val clearGraphBoolean = MappingPediaUtility.stringToBoolean(clearGraphString);
    //logger.info("clearGraphBoolean = " + clearGraphBoolean);

    val replaceMappingBaseURI = MappingPediaUtility.stringToBoolean(pReplaceMappingBaseURI);

    val manifestText = if(manifestFilePath != null ) {
      MappingDocumentController.getManifestContent(manifestFilePath);
    } else {
      null;
    }

    val manifestModel = if(manifestText != null) {
      this.virtuosoUtility.readModelFromString(manifestText, MappingPediaConstant.MANIFEST_FILE_LANGUAGE);
    } else {
      null;
    }

    //mappingpediaEngine.manifestModel = manifestModel;

    val oldMappingText:String = this.getMappingContent(manifestFilePath, pMappingFilePath);

    val mappingText = if(replaceMappingBaseURI) {
      MappingPediaUtility.replaceBaseURI(oldMappingText.split("\n").toIterator
        , newMappingBaseURI).mkString("\n");
    } else {
      oldMappingText;
    }

    val mappingDocumentModel = this.virtuosoUtility.readModelFromString(mappingText
      , MappingPediaConstant.MANIFEST_FILE_LANGUAGE);
    //mappingpediaEngine.mappingDocumentModel = mappingDocumentModel;

    //val virtuosoGraph = mappingpediaR2RML.getMappingpediaGraph();

    //val virtuosoGraph = MappingPediaUtility.getVirtuosoGraph(MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
    //, MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd, MappingPediaEngine.mappingpediaProperties.graphName);
    if(clearGraphBoolean) {
      try {
        this.virtuosoUtility.virtGraph.clear();
      } catch {
        case e:Exception => {
          logger.error("unable to clear the graph: " + e.getMessage);
        }
      }
    }

    if(manifestModel != null) {
      logger.info("Storing manifest triples.");
      val manifestTriples = MappingPediaUtility.toTriples(manifestModel);
      //logger.info("manifestTriples = " + manifestTriples.mkString("\n"));
      this.virtuosoUtility.store(manifestTriples, true, MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS);

      logger.info("Storing generated triples.");
      val additionalTriples = MappingDocumentController.generateAdditionalTriples(mappingLanguage
        , manifestModel, mappingDocumentModel);
      logger.info("additionalTriples = " + additionalTriples.mkString("\n"));

      this.virtuosoUtility.store(additionalTriples, true, MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS);
    }

    logger.info("Storing R2RML triples in Virtuoso.");
    val r2rmlTriples = MappingPediaUtility.toTriples(mappingDocumentModel);
    //logger.info("r2rmlTriples = " + r2rmlTriples.mkString("\n"));

    this.virtuosoUtility.store(r2rmlTriples, true, MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS);


  }

  def getR2RMLMappingDocumentFilePathFromManifestFile(manifestFilePath:String) : String = {
    logger.info("Reading manifest file : " + manifestFilePath);

    val manifestModel = this.virtuosoUtility.readModelFromFile(manifestFilePath, MappingPediaConstant.MANIFEST_FILE_LANGUAGE);

    val r2rmlResources = manifestModel.listResourcesWithProperty(
      RDF.`type`, MappingPediaConstant.MAPPINGPEDIAVOCAB_R2RMLMAPPINGDOCUMENT_CLASS);

    if(r2rmlResources != null) {
      val r2rmlResource = r2rmlResources.nextResource();

      val mappingDocumentFilePath = MappingPediaUtility.getFirstPropertyObjectValueLiteral(
        r2rmlResource, MappingPediaConstant.DEFAULT_MAPPINGDOCUMENTFILE_PROPERTY).toString();

      var mappingDocumentFile = new File(mappingDocumentFilePath.toString());
      val isMappingDocumentFilePathAbsolute = mappingDocumentFile.isAbsolute();
      val r2rmlMappingDocumentPath = if(isMappingDocumentFilePathAbsolute) {
        mappingDocumentFilePath
      } else {
        val manifestFile = new File(manifestFilePath);
        if(manifestFile.isAbsolute()) {
          manifestFile.getParentFile().toString() + File.separator + mappingDocumentFile;
        } else {
          mappingDocumentFilePath
        }
      }
      r2rmlMappingDocumentPath

    } else {
      val errorMessage = "mapping file is not specified in the manifest file";
      logger.error(errorMessage);
      throw new Exception(errorMessage);
    }

  }

  def getMappingContent(manifestFilePath:String, manifestText:String, pMappingFilePath:String, pMappingText:String):String = {

    val mappingContent:String = if(pMappingText == null) {
      val mappingFilePath = if(pMappingFilePath == null) {
        val mappingFilePathFromManifest = this.getR2RMLMappingDocumentFilePathFromManifestFile(manifestFilePath);
        mappingFilePathFromManifest;
      }  else {
        pMappingFilePath;
      }

      logger.info(s"reading r2rml file from $mappingFilePath ...");
      //val mappingFileContent = fromFile(mappingFilePath).getLines.mkString("\n");
      val mappingFileContent = scala.io.Source.fromURL(mappingFilePath).mkString; //DO NOT USE \n HERE!
      mappingFileContent;
    } else {
      pMappingText;
    }
    mappingContent;
  }

  def getMappingContent(manifestFilePath:String, pMappingFilePath:String):String = {
    this.getMappingContent(manifestFilePath, null, pMappingFilePath:String, null)
  }

  def getMappingContent(pMappingFilePath:String):String = {
    val mappingFileContent = fromFile(pMappingFilePath).getLines.mkString("\n");
    mappingFileContent;
  }

  def updateExistingMapping(mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String
                            //, mappingFileRef:MultipartFile
                            , mappingFile:File
                           ): GeneralResult = {
    logger.debug("mappingpediaUsername = " + mappingpediaUsername)
    logger.debug("mappingDirectory = " + mappingDirectory)
    logger.debug("mappingFilename = " + mappingFilename)
    try {
      //val mappingFile = MappingPediaUtility.multipartFileToFile(mappingFileRef, mappingDirectory)
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

}

object MappingDocumentController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def apply(): MappingDocumentController = {
    val propertiesFilePath = "/" + MappingPediaConstant.DEFAULT_CONFIGURATION_FILENAME;
    val url = getClass.getResource(propertiesFilePath)
    logger.info(s"loading mappingpedia-engine-mappings configuration file from: ${url}")
    val properties = new Properties();
    if (url != null) {
      val source = Source.fromURL(url)
      val reader = source.bufferedReader();
      properties.load(reader)
      logger.debug(s"properties.keySet = ${properties.keySet()}")
    }

    MappingDocumentController(properties)
  }

  def apply(properties: Properties): MappingDocumentController = {
    val ckanUtility = new MpcCkanUtility(
      properties.getProperty(MappingPediaConstant.CKAN_URL)
      , properties.getProperty(MappingPediaConstant.CKAN_KEY)
    );

    val githubUtility = new MpcGithubUtility(
      properties.getProperty(MappingPediaConstant.GITHUB_REPOSITORY)
      , properties.getProperty(MappingPediaConstant.GITHUB_USER)
      , properties.getProperty(MappingPediaConstant.GITHUB_ACCESS_TOKEN)
    );

    val virtuosoUtility = new MpcVirtuosoUtility(
      properties.getProperty(MappingPediaConstant.VIRTUOSO_JDBC)
      , properties.getProperty(MappingPediaConstant.VIRTUOSO_USER)
      , properties.getProperty(MappingPediaConstant.VIRTUOSO_PWD)
      , properties.getProperty(MappingPediaConstant.GRAPH_NAME)
    );
    val schemaOntology = MPCJenaUtility.loadSchemaOrgOntology(virtuosoUtility
      , MappingPediaConstant.SCHEMA_ORG_FILE, MappingPediaConstant.FORMAT)
    val jenaUtility = new MPCJenaUtility(schemaOntology);

    new MappingDocumentController(ckanUtility, githubUtility, virtuosoUtility, jenaUtility);

  }

  def generateManifestFile(mappingDocument: MappingDocument, datasetId: String, datasetPackageId:String) = {
    logger.info("GENERATING MANIFEST FILE FOR MAPPING DOCUMENT ...")
    logger.info(s"mappingDocument.hash = ${mappingDocument.hash}")


    val templateFiles = List(
      MappingPediaConstant.TEMPLATE_MAPPINGDOCUMENT_METADATA_NAMESPACE
      , MappingPediaConstant.TEMPLATE_MAPPINGDOCUMENT_METADATA);

    val mappingDocumentDateTimeSubmitted = MappingPediaEngine.sdf.format(new Date())

    val mapValues: Map[String, String] = Map(
      "$mappingDocumentID" -> mappingDocument.dctIdentifier
      , "$mappingDocumentTitle" -> mappingDocument.dctTitle
      , "$mappingDocumentDateTimeSubmitted" -> mappingDocumentDateTimeSubmitted
      , "$mappingDocumentCreator" -> mappingDocument.dctCreator
      , "$mappingDocumentSubjects" -> mappingDocument.dctSubject
      , "$mappingDocumentFilePath" -> mappingDocument.getDownloadURL()
      , "$datasetID" -> datasetId
      , "$mappingLanguage" -> mappingDocument.mappingLanguage
      , "$hash" -> mappingDocument.hash

      , "$ckanPackageID" -> datasetPackageId
      , "$ckanResourceID" -> mappingDocument.ckanResourceId

      //, "$datasetTitle" -> datasetTitle
      //, "$datasetKeywords" -> datasetKeywords
      //, "$datasetPublisher" -> datasetPublisher
      //, "$datasetLanguage" -> datasetLanguage
    );

    val filename = "metadata-mappingdocument.ttl";
    val generatedManifestFile = MappingPediaEngine.generateManifestFile(mapValues, templateFiles, filename, datasetId);
    logger.info("Manifest file generated.")
    generatedManifestFile
  }


  def detectMappingLanguage(mappingDocumentFile:File) : String = {
    if(mappingDocumentFile == null) {
      "r2rml"
    } else {
      val absolutePath = mappingDocumentFile.getAbsolutePath;
      this.detectMappingLanguage(absolutePath);
    }
  }

  def detectMappingLanguage(mappingDocumentDownloadURL:String) : String = {

    val mappingLanguage = if (mappingDocumentDownloadURL != null) {
      val splitedMappingLanguage = mappingDocumentDownloadURL.split("\\.")

      if (splitedMappingLanguage.length >= 3) {
        val extension1 = splitedMappingLanguage(splitedMappingLanguage.length-2);
        val extension2 = splitedMappingLanguage(splitedMappingLanguage.length-1);

        if("rml".equalsIgnoreCase(extension1) && "ttl".equalsIgnoreCase(extension2)) {
          "rml"
        } else {
          "r2rml"
        }
      } else {
        "r2rml"
      }
    } else {
      "r2rml"
    }
    mappingLanguage
  }

  def isOutdatedMappingDocument(pMdSubmitted:String, pDatasetModified:String) = {
    val mdSubmitted:Date = if(pMdSubmitted != null) {MappingPediaEngine.sdf.parse(pMdSubmitted); } else { null }
    val dsModified:Date = if(pDatasetModified != null) { MappingPediaEngine.sdf.parse(pDatasetModified); } else { null}

    val isOutdated = if(dsModified == null || mdSubmitted == null) { false }
    else {
      if( dsModified.after(mdSubmitted)) {
        true
      } else {
        false
      }
    }

    isOutdated
  }

  def getManifestContent(manifestFilePath:String):String = {
    this.getManifestContent(manifestFilePath, null);
  }

  def getManifestContent(manifestFilePath:String, manifestText:String):String = {
    logger.info("reading manifest  ...");
    val manifestContent:String = if(manifestText == null) {
      if(manifestFilePath == null) {
        val errorMessage = "no manifest is provided";
        logger.error(errorMessage);
        throw new Exception(errorMessage);
      } else {
        val manifestFileContent = fromFile(manifestFilePath).getLines.mkString("\n");
        //logger.info("manifestFileContent = \n" + manifestFileContent);
        manifestFileContent;
      }
    } else {
      manifestText;
    }
    manifestContent;
  }

  def generateAdditionalTriples(mappingLanguage:String, manifestModel:Model, mappingDocumentModel:Model) : List[Triple] = {
    if("rml".equalsIgnoreCase(mappingLanguage)) {
      this.generateAdditionalTriplesForRML(manifestModel, mappingDocumentModel);
    } else {
      this.generateAdditionalTriplesForR2RML(manifestModel, mappingDocumentModel);
    }
  }

  def generateAdditionalTriplesForR2RML(manifestModel:Model, mappingDocumentModel:Model) : List[Triple] = {
    logger.info("generating additional triples for R2RML ...");

    var newTriples:List[Triple] = List.empty;

    val r2rmlMappingDocumentResources = manifestModel.listResourcesWithProperty(
      RDF.`type`, MappingPediaConstant.MAPPINGPEDIAVOCAB_MAPPINGDOCUMENT_CLASS);

    if(r2rmlMappingDocumentResources != null) {
      while(r2rmlMappingDocumentResources.hasNext()) {
        val r2rmlMappingDocumentResource = r2rmlMappingDocumentResources.nextResource();
        //logger.info("r2rmlMappingDocumentResource = " + r2rmlMappingDocumentResource);

        //improve this code using, get all x from ?x rr:LogicalTable ?lt
        //mapping documents do not always explicitly have a TriplesMap
        //val triplesMapResources = mappingDocumentModel.listResourcesWithProperty(
        //  				RDF.`type`, MappingPediaConstant.R2RML_TRIPLESMAP_CLASS);
        val triplesMapResources = mappingDocumentModel.listResourcesWithProperty(
          MappingPediaConstant.R2RML_LOGICALTABLE_PROPERTY);
        //logger.info("triplesMapResources = " + triplesMapResources);
        if(triplesMapResources != null) {
          while(triplesMapResources.hasNext()) {
            val triplesMapResource = triplesMapResources.nextResource();
            val newStatement = new StatementImpl(r2rmlMappingDocumentResource
              , MappingPediaConstant.HAS_TRIPLES_MAPS_PROPERTY, triplesMapResource);
            //logger.info("adding new hasTriplesMap statement: " + newStatement);
            val newTriple = newStatement.asTriple();
            newTriples = newTriples ::: List(newTriple);
          }
        }
      }
    }

    logger.debug("newTriples = " + newTriples);
    newTriples;
  }

  def generateAdditionalTriplesForRML(manifestModel:Model, mappingDocumentModel:Model) : List[Triple] = {
    logger.info("generating additional triples for RML ...");

    val mappingDocumentResources = manifestModel.listResourcesWithProperty(
      RDF.`type`, MappingPediaConstant.MAPPINGPEDIAVOCAB_MAPPINGDOCUMENT_CLASS);

    val newTriples:List[Triple] = if(mappingDocumentResources != null) {
      mappingDocumentResources.toIterator.flatMap(mappingDocumentResource => {

        val triplesMapResources = mappingDocumentModel.listResourcesWithProperty(
          MappingPediaConstant.RML_LOGICALSOURCE_PROPERTY);


        if(triplesMapResources != null) {
          val newTriplesAux:List[Triple] = triplesMapResources.toIterator.map(triplesMapResource => {

            val newStatement = new StatementImpl(mappingDocumentResource
              , MappingPediaConstant.HAS_TRIPLES_MAPS_PROPERTY, triplesMapResource);
            val newTriple = newStatement.asTriple();
            newTriple
          }).toList;
          newTriplesAux
        } else {
          List.empty
        }
      }).toList;
    } else {
      List.empty
    }

    logger.info(s"newTriples = $newTriples");
    newTriples;
  }




}
