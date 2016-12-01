package de.appsist.service.lel.model;

public class Trigger
{
    public static final String PROCESS_CANCELLED = "appsist:event:processEvent:processCancelled";
    public static final String PROCESS_COMPLETE = "appsist:event:processEvent:processComplete";
    public static final String PROCESS_ERROR = "appsist:event:processEvent:processError";
    public static final String PROCESS_START = "appsist:event:processEvent:processStart";
    public static final String PROCESS_TERMINATED = "appsist:event:processEvent:processTerminated";

    public static final String CONTENT_SEEN = "appsist:content:contentSeen";

    public static final String INTERACTED_WITH = "appsist:item:interactedWith";

    public static final String USER_ONLINE = "appsist:user:userOnline";
    public static final String USER_OFFLINE = "appsist:user:userOffline";
}
 