package es.upm.fi.dia.oeg.mappingpedia.utility


import java.io.File
import java.net.HttpURLConnection
import java.util.Properties

import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.model.result.ListResult
import es.upm.fi.dia.oeg.mappingpedia.model.{Agent}
import es.upm.fi.dia.oeg.mappingpedia.utility.CKANUtility.logger
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine, MappingPediaProperties}
import eu.trentorise.opendata.jackan.CkanClient
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

import scala.collection.mutable.ListBuffer


class CKANUtility(val ckanUrl: String, val authorizationToken: String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val CKAN_API_ACTION_RESOURCE_CREATE = "/api/action/resource_create";
  val CKAN_API_ACTION_RESOURCE_UPDATE = "/api/action/resource_update";

  val CKAN_FIELD_NAME = "name";
  val CKAN_FIELD_DESCRIPTION = "description";
  val CKAN_FIELD_PACKAGE_ID = "package_id";
  val CKAN_FIELD_URL = "url";




  def updateResource(filePath: String, resourceId: String) : Integer = {
    val file = new File(filePath);
    this.updateResource(file, resourceId);
  }

  def updateResource(file:File, resourceId: String) : Integer = {
    val httpClient = HttpClientBuilder.create.build
    try {
      val uploadFileUrl = ckanUrl + "/api/action/resource_update"
      val httpPostRequest = new HttpPost(uploadFileUrl)
      httpPostRequest.setHeader("Authorization", authorizationToken)
      val mpEntity = MultipartEntityBuilder.create().addBinaryBody("upload", file)
        .addTextBody("id", resourceId).build();
      httpPostRequest.setEntity(mpEntity)
      val response = httpClient.execute(httpPostRequest)
      if (response.getStatusLine.getStatusCode < 200 || response.getStatusLine.getStatusCode >= 300) throw new RuntimeException("failed to add the file to CKAN storage. response status line from " + uploadFileUrl + " was: " + response.getStatusLine)
      val responseEntity = response.getEntity
      System.out.println(responseEntity.toString)
      HttpURLConnection.HTTP_OK
    } catch {
      case e: Exception => {
        e.printStackTrace()
        HttpURLConnection.HTTP_INTERNAL_ERROR
      }
    } finally if (httpClient != null) httpClient.close()

  }


  def addNewOrganization(organization:Agent) = {
    val jsonObj = new JSONObject();
    jsonObj.put("name", organization.foafName);

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionOrganizationCreate
    logger.info(s"Hitting endpoint: $uri");
    val response = Unirest.post(uri)
      .header("Authorization", this.authorizationToken)
      .body(jsonObj)
      .asJson();
    response;
  }


  def updateDatasetLanguage(organizationId:String, datasetId:String, language:String) : Integer = {
    val jsonObj = new JSONObject();

    jsonObj.put("owner_org", organizationId);
    jsonObj.put("name", datasetId);
    jsonObj.put("language", language);
    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionPackageUpdate
    logger.info(s"Hitting endpoint: $uri");
    logger.info(s"owner_org = $organizationId");
    logger.info(s"name = $datasetId");
    logger.info(s"language = $language");

    val response = Unirest.post(uri)
      .header("Authorization", this.authorizationToken)
      .body(jsonObj)
      .asJson();
    response.getStatus

  }

  def getResourcesUrlsAsListResult(resourcesIds:String) : ListResult[String]= {
    val resourcesUrls = this.getResourcesUrls(resourcesIds).asJava
    new ListResult[String](resourcesUrls.size(), resourcesUrls);
  }

  def getResourcesUrlsAsJava(resourcesIds:String) = {
    this.getResourcesUrls(resourcesIds).asJava
  }

  def getResourceIdByResourceUrl(packageId:String, pResourceUrl:String) : String = {
    val uri = s"${MappingPediaEngine.mappingpediaProperties.ckanActionPackageShow}?id=${packageId}"
    logger.info(s"Hitting endpoint: $uri");

    val response = Unirest.get(uri)
      .header("Authorization", this.authorizationToken)
      .asJson();
    val resources = response.getBody.getObject.getJSONObject("result").getJSONArray("resources");
    //logger.info(s"resources = $resources");

    var result:String = null;
    if(resources != null && resources.length() > 0) {
      for(i <- 0 until resources.length()) {
        val resource = resources.getJSONObject(i);
        val resourceUrl = resource.getString("url");

        if(pResourceUrl.trim.equals(resourceUrl)) {
          val resourceId = resource.getString("id");
          result = resourceId;
        }
      }
    }

    logger.info(s"result = $result");
    return result;
  }

  def getAnnotatedResourcesIdsAsListResult(packageId:String) : ListResult[String] = {
    val resultAux = this.getAnnotatedResourcesIds(packageId);
    new ListResult[String](resultAux);
  }

  def getAnnotatedResourcesIds(packageId:String) : List[String] = {
    logger.info(s"MappingPediaEngine.mappingpediaProperties = ${MappingPediaEngine.mappingpediaProperties}");

    val uri = s"${MappingPediaEngine.mappingpediaProperties.ckanActionPackageShow}?id=${packageId}"
    logger.info(s"Hitting endpoint: $uri");

    val response = Unirest.get(uri)
      .header("Authorization", this.authorizationToken)
      .asJson();
    val resources = response.getBody.getObject.getJSONObject("result").getJSONArray("resources");
    logger.info(s"resources = $resources");

    var resultsBuffer:ListBuffer[String] = new ListBuffer[String]();
    if(resources != null && resources.length() > 0) {
      for(i <- 0 until resources.length()) {
        val resource = resources.getJSONObject(i);
        if(resource.has(MappingPediaConstant.CKAN_RESOURCE_IS_ANNOTATED)) {
          val isAnnotatedString = resource.getString(MappingPediaConstant.CKAN_RESOURCE_IS_ANNOTATED);
          val isAnnotatedBoolean = MappingPediaUtility.stringToBoolean(isAnnotatedString);

          if(isAnnotatedBoolean) {
            val resourceId = resource.getString("id");
            resultsBuffer += resourceId;
          }
        }
      }
    }
    val results = resultsBuffer.toList

    logger.info(s"results = $results");
    return results;
  }

  def getResourcesUrls(resourcesIds:String) : List[String]= {
    val splitResourcesIds = resourcesIds.split(",").toList;

    if(splitResourcesIds.length == 1) {
      val resourceId = splitResourcesIds.iterator.next()
      val uri = s"${MappingPediaEngine.mappingpediaProperties.ckanActionResourceShow}?id=${resourceId}"
      logger.info(s"Hitting endpoint: $uri");
      val response = Unirest.get(uri)
        .header("Authorization", this.authorizationToken)
        .asJson();
      val resourceUrl = response.getBody.getObject.getJSONObject("result").getString("url");
      List(resourceUrl);
    } else {
      splitResourcesIds.flatMap(resourceId => { this.getResourcesUrls(resourceId)} )
    }
  }

  def getDatasets(organizationId:String) = {
    val uri = s"${MappingPediaEngine.mappingpediaProperties.ckanActionOrganizationShow}?id=$organizationId&include_datasets=true"
    logger.info(s"Hitting endpoint: $uri");

    val response = Unirest.get(uri)
      .header("Authorization", this.authorizationToken)
      .asJson();
    response
  }

  def updateDatasetLanguage(organizationId:String, language:String) : Integer = {
    val getDatasetsResponse = this.getDatasets(organizationId)
    val getDatasetsResponseStatus = getDatasetsResponse.getStatus
    if(getDatasetsResponseStatus >= 200 && getDatasetsResponseStatus < 300) {
      val packages = getDatasetsResponse.getBody.getObject.getJSONObject("result").getJSONArray("packages")
      for(i <- 0 to packages.length() - 1) {
        val pkg = packages.get(i)
        val datasetId = pkg.asInstanceOf[JSONObject].getString("id")
        logger.info(s"datasetId = $datasetId");

        this.updateDatasetLanguage(organizationId, datasetId, language);
      }
      HttpURLConnection.HTTP_OK

    } else {
      HttpURLConnection.HTTP_INTERNAL_ERROR
    }



  }
}

object CKANUtility {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def getDatasetList(catalogUrl:String) = {
    val cc: CkanClient = new CkanClient(catalogUrl)
    val datasetList = cc.getDatasetList.asScala

    logger.info(s"ckanDatasetList $catalogUrl = " + datasetList)
    new ListResult[String](datasetList.size, datasetList)
  }

  def getResult(response:CloseableHttpResponse) = {
    if(response == null) {
      null
    } else {
      val httpEntity  = response.getEntity
      val entity = EntityUtils.toString(httpEntity)
      val responseEntity = new JSONObject(entity);
      responseEntity.getJSONObject("result");
    }
  }

  def getResultId(response:CloseableHttpResponse) = {
    if(response == null) {
      null
    } else {
      val httpEntity  = response.getEntity
      val entity = EntityUtils.toString(httpEntity)
      val responseEntity = new JSONObject(entity);
      responseEntity.getJSONObject("result").getString("id");
    }
  }

  def getResultUrl(response:CloseableHttpResponse) = {
    if(response == null) {
      null
    } else {
      val httpEntity  = response.getEntity
      val entity = EntityUtils.toString(httpEntity)
      val responseEntity = new JSONObject(entity);
      responseEntity.getJSONObject("result").getString("url");
    }
  }

  def getResultPackageId(response:CloseableHttpResponse) = {
    if(response == null) {
      null
    } else {
      val httpEntity  = response.getEntity
      val entity = EntityUtils.toString(httpEntity)
      val responseEntity = new JSONObject(entity);
      responseEntity.getJSONObject("result").getString("package_id");
    }
  }
}
