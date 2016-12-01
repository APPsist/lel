package de.appsist.service.lel;

import java.util.*;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Verticle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.appsist.commons.event.*;
import de.appsist.commons.misc.StatusSignalConfiguration;
import de.appsist.commons.misc.StatusSignalSender;
import de.appsist.commons.util.EventUtil;
import de.appsist.service.lel.model.Trigger;

/*
 * This verticle is executed with the module itself, i.e. initializes all components required by the service.
 * The super class provides two main objects used to interact with the container:
 * - <code>vertx</code> provides access to the Vert.x runtime like event bus, servers, etc.
 * - <code>container</code> provides access to the container, e.g, for accessing the module configuration an the logging mechanism.  
 */
public class LELMainVerticle
    extends Verticle
{
	private JsonObject config;
	private RouteMatcher routeMatcher;

    // ontology prefix
    private final String ontologyPrefix = "http://www.appsist.de/ontology/";

    // logging facility
    private static final Logger log = LoggerFactory.getLogger(LELMainVerticle.class);
    
    // setting eventbus prefix for the appsist project
    final private static String eventbusPrefix = "appsist:";

    // save userId corresponding to sessionId
    // sessionId, userId pairs are added on login and removed on logout
    private final Map<String, String> sessionUsers = new HashMap<String, String>();

    // store demo users to prevent progress deletion of real users
    private final List<String> demoUsers = new ArrayList<String>();
	@Override
	public void start() {
		/* 
		 * - The module is executed programmatically with a configuration object. 
		 * - The (hardcoded) default configuration is applied if none of the above options has been applied.  
		 */
		if (container.config() != null && container.config().size() > 0) {
			config = container.config();
		} else {
            log.warn("Warning: No configuration applied! Using default settings.");
			config = getDefaultConfiguration();
		}
		/*
		 * In this method the verticle is registered at the event bus in order to receive messages. 
		 */
		initializeEventBusHandler();

        if (null != config.getArray("demousers")) {
            addDemoUsers(config.getArray("demousers"));
        }


		/*
		 * This block initializes the HTTP interface for the service. 
		 */
		initializeHTTPRouting();
		vertx.createHttpServer()
			.requestHandler(routeMatcher)
			.listen(config.getObject("webserver")
			.getInteger("port"));
		
        log.info("APPsist service \"LearnEventListener-Service\" has been initialized with the following configuration:\n" + config.encodePrettily());
        
        JsonObject statusSignalObject = config.getObject("statusSignal");
        StatusSignalConfiguration statusSignalConfig;
        if (statusSignalObject != null) {
          statusSignalConfig = new StatusSignalConfiguration(statusSignalObject);
        } else {
          statusSignalConfig = new StatusSignalConfiguration();
        }

        StatusSignalSender statusSignalSender =
          new StatusSignalSender("lel", vertx, statusSignalConfig);
        statusSignalSender.start();

	}
	
    private void addDemoUsers(JsonArray demoUserArray)
    {
        Iterator<Object> demoUserIterator = demoUserArray.iterator();
        while (demoUserIterator.hasNext()) {
            demoUsers.add(demoUserIterator.next().toString());
        }
        log.debug("demousers: " + demoUsers.toString());
    }

	@Override
	public void stop() {
        log.info("APPsist service \"LearnEventListener-Service\" has been stopped.");
	}
	
	/**
	 * Create a configuration which used if no configuration is passed to the module.
	 * @return Configuration object.
	 */
	private static JsonObject getDefaultConfiguration() {
		JsonObject defaultConfig =  new JsonObject();
		JsonObject webserverConfig = new JsonObject();
        webserverConfig.putNumber("port", 8090);
		webserverConfig.putString("statics", "www");
        webserverConfig.putString("basePath", "/services/lel");
		defaultConfig.putObject("webserver", webserverConfig);
		return defaultConfig;
	}
	
	/**
	 * In this method the handlers for the event bus are initialized.
	 */
	private void initializeEventBusHandler() {
        Handler<Message<JsonObject>> lelTriggerHandler = new Handler<Message<JsonObject>>()
        {
			
			@Override
			public void handle(Message<JsonObject> message) {
				JsonObject messageBody = message.body();
				log.info(message.address());
				switch (message.address()) {
                    case Trigger.PROCESS_START :
                        ProcessStartEvent pse = EventUtil.parseEvent(messageBody.toMap(),
                                ProcessStartEvent.class);
                        handleProcessStartEvent(pse);
					break;

                    case Trigger.PROCESS_ERROR :
                        ProcessErrorEvent pee = EventUtil.parseEvent(messageBody.toMap(),
                                ProcessErrorEvent.class);
                        handleProcessErrorEvent(pee);
					break;

                    case Trigger.PROCESS_CANCELLED :
                        ProcessCancelledEvent pcae = EventUtil.parseEvent(messageBody.toMap(),
                                ProcessCancelledEvent.class);
                        handleProcessCancelledEvent(pcae);
                        break;
                    case Trigger.PROCESS_COMPLETE :
                        ProcessCompleteEvent pce = EventUtil.parseEvent(messageBody.toMap(),
                                ProcessCompleteEvent.class);
                        handleProcessCompleteEvent(pce);
                        break;
                    case Trigger.PROCESS_TERMINATED :
                        ProcessTerminateEvent pte = EventUtil.parseEvent(messageBody.toMap(),
                                ProcessTerminateEvent.class);
                        handleProcessTerminateEvent(pte);
                        break;
                    case Trigger.CONTENT_SEEN :
                        log.info("contentSeenMessage: " + messageBody.encodePrettily());
                        handleContentSeen(messageBody);
                        break;
                    case Trigger.INTERACTED_WITH :
                        handleInteractedWith(messageBody);
                        break;
                    case Trigger.USER_OFFLINE :
                        log.debug("handleUserOffline: " + messageBody.encodePrettily());
                        handleUserOffline(messageBody);
                        break;

                    default :
                        log.error("Unknown Trigger:" + message.address());
                        break;

				}

                log.debug("Received a message on address: " + message.address());
			}
		};
		
        vertx.eventBus().registerHandler(Trigger.PROCESS_CANCELLED, lelTriggerHandler);
        vertx.eventBus().registerHandler(Trigger.PROCESS_COMPLETE, lelTriggerHandler);
        vertx.eventBus().registerHandler(Trigger.PROCESS_ERROR, lelTriggerHandler);
        vertx.eventBus().registerHandler(Trigger.PROCESS_START, lelTriggerHandler);
        vertx.eventBus().registerHandler(Trigger.CONTENT_SEEN, lelTriggerHandler);
        vertx.eventBus().registerHandler(Trigger.INTERACTED_WITH, lelTriggerHandler);
        vertx.eventBus().registerHandler(Trigger.USER_OFFLINE, lelTriggerHandler);

        Handler<Message<JsonObject>> userOnlineEventHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> jsonMessage)
            {
                log.debug(" userOfflineEventHandler jsonMessage"
                            + jsonMessage.toString());
                UserOnlineEvent userOnlineEvent = EventUtil.parseEvent(jsonMessage.body().toMap(),
                        UserOnlineEvent.class);
                processUserOnlineEvent(userOnlineEvent);
            }
        };

        vertx.eventBus().registerHandler(eventbusPrefix + "event:userOnline",
                userOnlineEventHandler);

        Handler<Message<JsonObject>> userOfflineEventHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> jsonMessage)
            {
                log.debug(" userOfflineEventHandler jsonMessage"
                            + jsonMessage.toString());
                UserOfflineEvent userOfflineEvent = EventUtil.parseEvent(jsonMessage.body().toMap(),
                        UserOfflineEvent.class);
                processUserOfflineEvent(userOfflineEvent);
            }
        };

        vertx.eventBus().registerHandler(eventbusPrefix + "event:userOffline",
                userOfflineEventHandler);
	}
	
	/**
	 * In this method the HTTP API build using a route matcher.
	 */
	private void initializeHTTPRouting() {
		routeMatcher = new RouteMatcher();
        final String staticFileDirectory = config.getObject("webserver").getString("statics");
		
		/*
		 * The following rules are applied in the order given during the initialization.
		 * The first rule which matches the request is applied and the latter rules are ignored. 
		 */
		
		/*
		 * This rule applies to all request of type GET to a path like "/entries/abc".
		 * The path segment "abc" is being made available as request parameter "id".
		 */
		routeMatcher.get("/entries/:id", new Handler<HttpServerRequest>() {
			
			@Override
			public void handle(HttpServerRequest request) {
				String id = request.params().get("id");
				request.response().end("Received request for entry '" + id + "'.");
			}
		});
		
		/*
		 * This rule applies to all request of type GET to a path like "/entries/abc".
		 * The path segment "abc" is being made available as request parameter "id".
		 */
		routeMatcher.get("/services/lel/contactSprout", new Handler<HttpServerRequest>() {
			
			@Override
			public void handle(HttpServerRequest req) {
				String requestURL = "/index/assessment/";
	    		HttpClient client =  vertx.createHttpClient().setPort(22211).setHost("192.168.1.21").setKeepAlive(false);
	    		 
	 			HttpClientRequest request = client.post(requestURL,new Handler<HttpClientResponse>(){

	    			@Override
	    			public void handle(HttpClientResponse response) {
	    				log.info("Response: "+ response.statusCode());
	    			}
	    		});
				request.setChunked(true);
	    		request.end();
	    		client.close();
	    		
	    		req.response().end("tried to contact Sprout");
			}
		});
		
		/*
		 * This entry serves files from a directory specified in the configuration.
		 * In the default configuration, the files are served from "src/main/resources/www", which is packaged with the module. 
		 */
		routeMatcher.getWithRegEx("/.*", new Handler<HttpServerRequest>() {
			
			@Override
			public void handle(HttpServerRequest request) {
                request.response().sendFile(staticFileDirectory + request.path());
			}
		});
	}

    // when we receive a content seen message on the eventbus we add an experienced statement
    // to the Learning Record Store
    // mandatory information sessionId && contentId

    private void handleContentSeen(JsonObject contentSeenMessage)
    {
        String sId = contentSeenMessage.getString("sessionId");
        String uId = sessionUsers.get(sId);
        String token = contentSeenMessage.getString("token");

        final String verb = "http://adlnet.gov/expapi/verbs/experienced";
        String object = contentSeenMessage.getString("contentId");
        
        if (object.indexOf("d5b504eb-a4f3-4646-bbe7-99ebb8ab9414") != -1){
        	String requestURL = "/index/qa/?blip=blup";
    		HttpClient client =  vertx.createHttpClient().setPort(22211).setHost("192.168.1.21").setKeepAlive(false);
    		 
 			HttpClientRequest request = client.post(requestURL,new Handler<HttpClientResponse>(){

    			@Override
    			public void handle(HttpClientResponse response) {
    				log.info("Response: "+ response.statusCode());
    			}
    		});
			request.setChunked(true);
    		request.end();
    		client.close();
    		//log.info(actor+" completed process "+ object);
        }
        
                
        // check if contentId is absolute
        if (!object.startsWith(ontologyPrefix) && !object.startsWith("file:///")) {
            findFullContentId(sId, token, uId, verb, object);
        }
        else {
            spreadInformation(uId, verb, object);
        }
    }


    // when we receive a content seen message on the eventbus we add an experienced statement
    // to the Learning Record Store
    // mandatory information sessionId && contentId

    private void handleInteractedWith(JsonObject interactedWithMessage)
    {
        String sId = interactedWithMessage.getString("sessionId");
        String token = interactedWithMessage.getString("token");

        final String verb = "http://adlnet.gov/expapi/verbs/interacted";
        // TODO: expect to receive an array with entries for items the user interacted with
        // during assistance
        // TODO: implement LRS statement creation
    }

    private void handleProcessStartEvent(ProcessStartEvent pse)
    {
        String verb = "http://adlnet.gov/expapi/verbs/attempted";
        String actor = pse.getUserId();
        String activityId = pse.getProcessId();
        log.debug(" - process start  received activityId: " + activityId);

        // check if processID is absolute

        if (!activityId.startsWith(ontologyPrefix)) {
            findFullActivityId(actor, verb, activityId);
        }
        else {
            // findProductionItems(activityId);
            spreadInformation(actor, verb, activityId);
        }


    }

    private void handleProcessCompleteEvent(ProcessCompleteEvent pce)
    {
        String verb = "http://adlnet.gov/expapi/verbs/completed";
        String actor = pce.getUserId();
        String object = pce.getProcessId();
        // check if processID is absolute
        increaseProcessCompleteUserModelValue(actor, object);
        if (!object.startsWith(ontologyPrefix)) {
            findFullActivityId(actor, verb, object);
        }

        else {
            spreadInformation(actor, verb, object);
        }
    }

    private void increaseProcessCompleteUserModelValue(String actor, String object) {
		// TODO Auto-generated method stub
    	String requestURL = "/services/usermodel/users/"+actor+"/measures/"+object;
		HttpClient client =  vertx.createHttpClient().setPort(8080).setHost("localhost").setKeepAlive(false);
		HttpClientRequest request = client.post(requestURL,new Handler<HttpClientResponse>(){

			@Override
			public void handle(HttpClientResponse response) {
				//log.info("Response: "+ response.statusCode());
			}
			
		});
		request.end();
		client.close();
		//log.info(actor+" completed process "+ object);
	}

	private void handleProcessTerminateEvent(ProcessTerminateEvent pte)
    {
        String verb = "http://adlnet.gov/expapi/verbs/completed";
        String actor = pte.getUserId();
        String object = pte.getProcessId();
        // check if processID is absolute
        if (!object.startsWith(ontologyPrefix)) {
            findFullActivityId(actor, verb, object);
        }
        else {
            spreadInformation(actor, verb, object);
        }
    }

    private void handleProcessErrorEvent(ProcessErrorEvent pee)
    {
        String verb = "http://adlnet.gov/expapi/verbs/failed";
        String actor = pee.getUserId();
        String object = pee.getProcessId();
        // check if processID is absolute
        if (!object.startsWith(ontologyPrefix)) {
            findFullActivityId(actor, verb, object);
        }

        else {
            spreadInformation(actor, verb, object);
        }
    }

    private void handleProcessCancelledEvent(ProcessEvent pcae)
    {
        String verb = "http://purl.org/xapi/adl/verbs/abandoned";
        String actor = pcae.getUserId();
        String object = pcae.getProcessId();
        // check if processID is absolute
        if (!object.startsWith(ontologyPrefix)) {
            findFullActivityId(actor, verb, object);
        }

        else {
            spreadInformation(actor, verb, object);
        }
    }

    private void handleUserOffline(JsonObject userOfflineMessage)
    {
        String userId = userOfflineMessage.getString("userId", "catweazle");
        if (!userId.equals("catweazle")) {
            if (demoUsers.contains(userId)) {
                log.debug(" trigger voiding of " + userId + "s progress");
                vertx.eventBus().send("appsist:query:lrs#voidLearningProgress", userOfflineMessage);
            }
        }
    }

    private void spreadInformation(String actor, String verb, String object)
    {
        if (validStatementParameters(actor, verb, object)) {
            JsonObject jo = new JsonObject();
            jo.putString("agent", actor);
            jo.putString("verb", verb);
            jo.putString("activityId", object);
            if (!demoUsers.contains(actor) && verb.indexOf("completed") != -1) {
                JsonObject mastersJsonObject = new JsonObject();
                mastersJsonObject.putString("userId", actor);
                mastersJsonObject.putString("processId", object);
                // TODO: implement to comply to security requirements
                // mastersJsonObject.putString("sid", sessionId);
                // mastersJsonObject.putString("token", "token");
                vertx.eventBus().send("appsist:service:usermodel#mastersProcess",
                        mastersJsonObject);
            }
            vertx.eventBus().send("appsist:query:lrs#buildAndStoreStatement", jo);
        }
        else {
            log.error(" - Invalid statement parameters: actor - " + actor + " | verb - " + verb
                    + " | object - " + object);
        }

    }

    private boolean validStatementParameters(String actor, String verb, String objectId)
    {
        log.debug(" (actor|verb|objectId) - " + actor + " | " + verb + " | " + objectId);
        boolean validationResult = true;
        if (null == actor || null == verb || null == objectId) {
            // invalid statement
            validationResult = false;
        }

        if (actor.equals("") || verb.equals("") || objectId.equals("")) {
            // invalid statement
            validationResult = false;
        }
        return validationResult;
    }

    // find full URI of measure ID
    private void findFullActivityId(final String actor, final String verb, final String activityId)
    {

        JsonObject message = new JsonObject();

        String sparqlQueryForMeasureId = "PREFIX app: <http://www.appsist.de/ontology/> PREFIX terms: <http://purl.org/dc/terms/> "
                + " SELECT DISTINCT ?uri WHERE { ?uri a ?_ FILTER (REGEX(str(?uri),'" + activityId
                + "$')) }";
        log.debug(" sending SPARQL query: " + sparqlQueryForMeasureId);
        JsonObject sQuery = new JsonObject();
        sQuery.putString("query", sparqlQueryForMeasureId);
        message.putObject("sparql", sQuery);
        vertx.eventBus().send(eventbusPrefix + "requests:semwiki", message,
                new Handler<Message<String>>()
                {
                    public void handle(Message<String> reply)
                    {
                        List<String> foundMeasureIds = new ArrayList<String>();
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode root = mapper.readTree(reply.body());
                            if (null != root) {
                                foundMeasureIds = root.findValuesAsText("value");
                            }
                            if (!foundMeasureIds.isEmpty()) {
                                String fullMeasureId = foundMeasureIds.get(0);
                                if ("http://adlnet.gov/expapi/verbs/completed".equals(verb)) {
                                    // findProductionItems(fullMeasureId);
                                }

                                spreadInformation(actor, verb, fullMeasureId);
                            }
                            else {
                                log.error(" no URI found in ontology for content: "
                                        + activityId);
                            }
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                    };
                });
    }

    // find full URI of measure ID
    private void findFullContentId(final String sId, final String token, final String uId,
            final String verb,
            final String contentId)
    {

        JsonObject message = new JsonObject();

        String sparqlQueryForMeasureId = "PREFIX app: <http://www.appsist.de/ontology/> PREFIX terms: <http://purl.org/dc/terms/> "
                + " SELECT DISTINCT ?uri WHERE { ?uri a ?_ FILTER (REGEX(str(?uri),'" + contentId
                + "$')) }";
        log.debug(" sending SPARQL query: " + sparqlQueryForMeasureId);
        JsonObject sQuery = new JsonObject();
        sQuery.putString("query", sparqlQueryForMeasureId);
        message.putObject("sparql", sQuery);
        vertx.eventBus().send(eventbusPrefix + "requests:semwiki", message,
                new Handler<Message<String>>()
                {
                    public void handle(Message<String> reply)
                    {
                        List<String> foundContentIds = new ArrayList<String>();
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode root = mapper.readTree(reply.body());
                            if (null != root) {
                                foundContentIds = root.findValuesAsText("value");
                            }
                            if (!foundContentIds.isEmpty()) {
                                String fullContentId = foundContentIds.get(0);
                                log.debug(" fullContentId: " + fullContentId);
                                spreadInformation(uId, verb, fullContentId);
                            }
                            else {
                                log.error(
" no URI found in ontology for content: " + contentId);
                            }
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                    };
                });
    }

    // ==================================
    //
    // User on/offline handlers
    //
    // ==================================

    // after user logs in -> store user and session
    private void processUserOnlineEvent(UserOnlineEvent userOnlineEvent)
    {
        if (null != userOnlineEvent) {
            String user = userOnlineEvent.getUserId();
            String session = userOnlineEvent.getSessionId();

            if (!sessionUsers.containsKey(session)) {
                sessionUsers.put(session, user);
            }
            else {
                if (!user.equals(sessionUsers.get(session))) {
                    // most probably user logout was not received
                    // replace with new user Id
                    sessionUsers.remove(session);
                    sessionUsers.put(session, user);
                }
            }

        }
        else {
            log.error("invalid user online event received");
        }
    }

    // remove user from map after logout
    private void processUserOfflineEvent(UserOfflineEvent userOfflineEvent)
    {
        if (null != userOfflineEvent) {
            String session = userOfflineEvent.getSessionId();
            if (sessionUsers.containsKey(session)) {
                sessionUsers.remove(session);
            }
            else {
                log.warn(
                        "LearnEventListener - Attempt to remove not logged in user with sessionId: "
                                + session);
            }
        }
    }
}
