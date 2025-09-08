graph TD
    subgraph "User & Viewer"
        A["Broadcasting Software <br> (OBS)"]
        B["Viewer's Web Browser"]
    end

    subgraph "AWS IVS Platform"
        C["IVS Ingest Endpoint"]
        D["Amazon IVS Channel"]
        E["IVS Chat Room"]
    end

    subgraph "WV Backend"
        F["AwsEventBridgeController <br> Catches 'Stream Start' event"]
        H["livestream_transcriptions"]
        I["WV API Endpoint <br> /livestreams-v2/${livestreamID}/rtmps-transcription"]
    end

    subgraph "AWS Fargate (Worker Task)"
        J["Fargate Task <br> (Node.js/TypeScript)"]
        K["FFmpeg Audio Extraction"]
    end

    subgraph "AWS Transcribe"
        L["Transcription Service"]
    end

    subgraph "AWS Translate"
        M["Translate Service"]
    end

    subgraph "Security & Authentication"
        N["Zoom CSMS Bridge <br> (Holds JWT Secret)"]
    end

    %% --- The Flow ---

    A -- "1.Sends RTMP Stream" --> C
    C -- "2.Streams Data" --> D
    D -- "3.Emits 'Stream Start' Event" --> F
    F -- "4.Starts Task with Vars" --> J

    J -- "5.Connects to Playback URL" --> D
    J -- "6.Spawns FFmpeg" --> K
    K -- "7.Pipes Audio Stream" --> L
    L -- "8.Sends Transcript" --> J
    J -- "9.(Optional) Sends for Translation" --> M
    M -- "10.Returns Translation" --> J

    J -- "11.Sends Event to Chat" --> E
    E -- "Pushes to Viewer via WebSocket" --> B

    J -- "12.Puts Metadata" --> D

    J -- "13.Fetches JWT Secret" --> N
    J -- "14.Generates JWT & Calls API" --> I
    I -- "15.Verifies JWT using secret from" --> N
    I -- "16.Saves transcript" --> H

    B -- "Watches Video" --> D