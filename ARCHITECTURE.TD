graph TD
    subgraph "User & Viewer"
        ENCODER["Broadcasting Software <br> (OBS)"]
        VIEWER["Viewer's Web Browser"]
    end

    subgraph "AWS IVS Platform"
        IVS_INGEST["IVS Ingest Endpoint"]
        IVS_CHANNEL["Amazon IVS Channel"]
        IVS_CHATROOM["IVS Chat Room"]
    end

    subgraph "WV Backend"
        CONTROLLER["AwsEventBridgeController <br> Catches 'Stream Start' event"]
        DB["livestream_transcriptions"]
        ENDPOINT["WV API Endpoint <br> /livestreams-v2/${livestreamID}/rtmps-transcription"]
    end

    subgraph "AWS Fargate (Worker Task)"
        FARGATE_TASK["Fargate Task <br> (Node)"]
        FFMPEG["FFmpeg"]
    end

    subgraph "AWS Transcribe"
        TRANSCRIBE["Transcription Service"]
    end

    subgraph "AWS Translate"
        TRANSLATE["Translate Service"]
    end

    subgraph "Security & Authentication"
        CSMS["Zoom CSMS Bridge <br> (Holds JWT Secret)"]
    end

    subgraph "Job Queue"
        SQS[SQS Jobs Queue]
    end

    %% --- The Flow ---

    ENCODER -- "1. Sends RTMP Stream" --> IVS_INGEST
    IVS_INGEST -- "2. Streams Data" --> IVS_CHANNEL
    IVS_CHANNEL -- "3. Emits 'Stream Start' Event" --> CONTROLLER
    CONTROLLER -- "4. Adds job to queue with Vars" --> SQS
    FARGATE_TASK -- "5. Pulls job from queue" --> SQS

    FARGATE_TASK -- "6. Connects to Playback URL" --> IVS_CHANNEL
    FARGATE_TASK -- "7. Spawns FFmpeg" --> FFMPEG
    FFMPEG -- "8. Pipes Audio Stream" --> TRANSCRIBE
    TRANSCRIBE -- "9. Returns Transcript" --> FARGATE_TASK
    FARGATE_TASK -- "10. (Optional) Translates Transcript" --> TRANSLATE
    TRANSLATE -- "11. Returns Translation" --> FARGATE_TASK

    FARGATE_TASK -- "12. Sends IVS Chat Event" --> IVS_CHATROOM
    FARGATE_TASK -- "13. Injects Timed Metadata" --> IVS_CHANNEL
    FARGATE_TASK -- "14. Calls API to Save Transcript" --> ENDPOINT

    IVS_CHATROOM -- "15. Pushes Event to Viewer" --> VIEWER
    ENDPOINT -- "16. Verifies JWT via" --> CSMS
    ENDPOINT -- "17. Saves to DB" --> DB

    VIEWER -- "Watches IVS Video Stream" --> IVS_CHANNEL