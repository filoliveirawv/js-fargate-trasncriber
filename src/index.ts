import { spawn, type ChildProcessWithoutNullStreams } from "child_process";
import { type Readable } from "stream";
import { IvschatClient, SendEventCommand } from "@aws-sdk/client-ivschat";
import {
  TranscribeStreamingClient,
  StartStreamTranscriptionCommand,
  type Result,
  type LanguageCode,
} from "@aws-sdk/client-transcribe-streaming";
import {
  TranslateClient,
  TranslateTextCommand,
} from "@aws-sdk/client-translate";
import axios from "axios";

type EnvVar = string | undefined;
type LanguageEnvVar = LanguageCode | undefined;

// Hard limit from Amazon Transcribe
const MAX_CHUNK_SIZE = 32_000;

// -- TRANSCRIPTION SETUP  --
const setupTranscription = async ({
  transcribeClient,
  audioStream,
  languageCode,
}: {
  transcribeClient: TranscribeStreamingClient;
  audioStream: Readable;
  languageCode: LanguageCode;
}) => {
  console.log("Setting up transcription...");

  try {
    const audioStreamGenerator = async function* () {
      for await (const chunk of audioStream) {
        let offset = 0;
        while (offset < chunk.length) {
          const end = Math.min(offset + MAX_CHUNK_SIZE, chunk.length);
          yield { AudioEvent: { AudioChunk: chunk.slice(offset, end) } };
          offset = end;
        }
      }
    };

    const command = new StartStreamTranscriptionCommand({
      LanguageCode: languageCode,
      MediaSampleRateHertz: 16000,
      MediaEncoding: "pcm",
      AudioStream: audioStreamGenerator(),
    });

    return await transcribeClient.send(command);
  } catch (error) {
    console.error("Error setting up transcription:", error);
    throw error;
  }
};

// -- FFMPEG SETUP --
const setupFFmpeg = ({
  playbackUrl,
  playbackJWT,
}: {
  playbackUrl: string;
  playbackJWT: string;
}) => {
  console.log("Setting up FFmpeg process...");

  const ffmpegArgs: string[] = [
    // -- Lowlatency Flags --
    "-fflags",
    "nobuffer",
    // -- Reconnect Flags --
    "-reconnect",
    "1",
    "-reconnect_streamed",
    "1",
    "-reconnect_delay_max",
    "2",
    // -- Standard Flags --
    "-i",
    `${playbackUrl}?token=${playbackJWT}`,
    "-vn", // no video
    "-acodec",
    "pcm_s16le", // raw audio format
    "-ar",
    "16000", // 16kHz sample rate
    "-ac",
    "1", // mono channel
    "-f",
    "s16le", // Format for streaming
    "pipe:1", // output to stdout
  ];

  const ffmpegProcess: ChildProcessWithoutNullStreams = spawn(
    "ffmpeg",
    ffmpegArgs
  );

  // we need to keep this to drain the buffer
  // this keeps the pipeline open and the audio flowing
  ffmpegProcess.stderr.on("data", (data: Buffer) => {});
  ffmpegProcess.on("error", (error: Error) => {
    console.error("FFmpeg process error:", error);
  });
  ffmpegProcess.on("close", (code: number) => {
    if (code === 0) {
      console.log("FFmpeg process completed successfully.");
    } else {
      console.warn(`FFmpeg process exited with code ${code}.`);
    }
  });

  return ffmpegProcess;
};

const saveTranscriptToDB = async ({
  transcript,
  languageCode,
  apiToken,
  apiUrl,
}: {
  transcript: string;
  languageCode: string;
  apiToken: string;
  apiUrl: string;
}) => {
  if (!apiUrl || !apiToken) {
    console.error("Missing Laravel API environment variables.");
    return;
  }

  try {
    await axios.post(
      apiUrl,
      { transcript, languageCode },
      {
        headers: {
          Authorization: `Bearer ${apiToken}`,
          Accept: "application/json",
        },
      }
    );
    console.log("Successfully saved transcript");
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(
        "Failed to save transcript to DB (Axios Error):",
        error.response?.data || error.message
      );
    } else if (error instanceof Error) {
      console.error("Failed to save transcript to DB (Error):", error.message);
    } else {
      console.error(
        "An unknown error occurred while saving transcript:",
        error
      );
    }
  }
};

//  -- TRANSCRIPT EVENT SENDER --
const sendTranscriptEvent = async ({
  ivsChatClient,
  roomArn,
  transcript,
  firstResult,
  languageCode,
}: {
  ivsChatClient: IvschatClient;
  roomArn: string;
  transcript: string;
  firstResult: Result;
  languageCode: LanguageCode;
}) => {
  if (!firstResult.ResultId) {
    console.error("Missing ResultId in firstResult.");
    return;
  }

  const command = new SendEventCommand({
    roomIdentifier: roomArn,
    eventName: "Transcript Update",
    attributes: {
      transcript: transcript,
      isPartial: String(firstResult.IsPartial),
      resultId: firstResult.ResultId,
      languageCode: languageCode,
    },
  });

  let attempts = 0;
  const maxAttempts = 3;
  while (attempts < maxAttempts) {
    try {
      await ivsChatClient.send(command);
      return;
    } catch (err) {
      attempts++;
      console.error(
        `Failed to send IVS Chat event (attempt ${attempts}):`,
        err
      );
      if (attempts >= maxAttempts) {
        console.error("Giving up after 3 attempts.");
      } else {
        await new Promise((resolve) =>
          setTimeout(resolve, 500 * 2 ** attempts)
        );
      }
    }
  }
};

// -- TRANSLATE AND SEND EVENT
const handleTranslation = async ({
  ivsChatClient,
  ivsChatRoomArn,
  translateClient,
  transcript,
  fromLang,
  toLang,
  firstResult,
  apiToken,
  apiUrl,
}: {
  ivsChatClient: IvschatClient;
  ivsChatRoomArn: string;
  translateClient: TranslateClient;
  transcript: string;
  fromLang: LanguageCode;
  toLang: LanguageCode;
  firstResult: Result;
  apiToken: string;
  apiUrl: string;
}) => {
  let translatedText = transcript;

  try {
    const command = new TranslateTextCommand({
      Text: transcript,
      SourceLanguageCode: fromLang,
      TargetLanguageCode: toLang,
    });
    const response = await translateClient.send(command);
    translatedText = response.TranslatedText || transcript;
  } catch (error) {
    console.error("Error translating text:", error);
  }

  sendTranscriptEvent({
    ivsChatClient,
    roomArn: ivsChatRoomArn,
    transcript: translatedText,
    firstResult,
    languageCode: toLang,
  });

  if (!firstResult.IsPartial) {
    saveTranscriptToDB({
      transcript,
      languageCode: toLang,
      apiToken,
      apiUrl,
    });
  }
};

// -- MAIN APPLICATION LOGIC --
process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Promise Rejection:", reason);
  process.exit(1);
});

const main = async () => {
  console.log("-- Fargate Task Started --");

  const playbackUrl: EnvVar = process.env.PLAYBACK_URL;
  const awsRegion: EnvVar = process.env.AWS_REGION;
  const ivsChatRoomArn: EnvVar = process.env.IVS_CHAT_ROOM_ARN;
  const playbackJWT: EnvVar = process.env.PLAYBACK_JWT;
  const fromLang: LanguageEnvVar =
    (process.env.FROM_LANG as LanguageCode) || "en-IE";
  const toLang: LanguageEnvVar =
    (process.env.TO_LANG as LanguageCode) || "en-IE";
  const laravelAPIEndpoint: EnvVar = process.env.LARAVEL_API_ENDPOINT;
  const laravelAPIToken: EnvVar = process.env.LARAVEL_API_TOKEN;

  if (
    !playbackUrl ||
    !ivsChatRoomArn ||
    !awsRegion ||
    !playbackJWT ||
    !laravelAPIEndpoint ||
    !laravelAPIToken
  ) {
    console.error("Missing required environment variables.");
    process.exit(1);
  }

  let ivsChatClient: IvschatClient | undefined;
  let transcribeClient: TranscribeStreamingClient | undefined;
  let ffmpegProcess: ChildProcessWithoutNullStreams | undefined;
  let translateClient: TranslateClient | undefined;

  const cleanup = async () => {
    if (ffmpegProcess && !ffmpegProcess.killed) {
      console.log("Cleaning up FFmpeg process...");
      ffmpegProcess.kill("SIGTERM");
    }
    if (ivsChatClient) {
      console.log("Destroying IVS Chat client...");
      ivsChatClient.destroy();
    }
    if (transcribeClient) {
      console.log("Destroying Transcribe client...");
      transcribeClient.destroy();
    }
    if (translateClient) {
      console.log("Destroying Translate client...");
      translateClient.destroy();
    }
  };

  process.on("SIGINT", async () => {
    console.log("Received SIGINT. Shutting down gracefully...");
    await cleanup();
    process.exit(0);
  });
  process.on("SIGTERM", async () => {
    console.log("Received SIGTERM. Shutting down gracefully...");
    await cleanup();
    process.exit(0);
  });

  // Init IVS Chat Client
  try {
    ivsChatClient = new IvschatClient({ region: awsRegion });
  } catch (err) {
    console.error("Failed to instantiate IVS Chat client:", err);
    await cleanup();
    process.exit(1);
  }

  // Init Transcribe Client
  try {
    transcribeClient = new TranscribeStreamingClient({ region: awsRegion });
  } catch (err) {
    console.error("Failed to instantiate Transcribe client:", err);
    await cleanup();
    process.exit(1);
  }

  // Init Translate Client
  const needsTranslation = fromLang !== toLang;
  if (needsTranslation) {
    console.log("Setting up translation...");
    try {
      // init translate client
      translateClient = new TranslateClient({ region: awsRegion });
    } catch (err) {
      console.error("Failed to instantiate Translate client:", err);
      await cleanup();
      process.exit(1);
    }
  }

  // Start process
  try {
    ffmpegProcess = setupFFmpeg({ playbackUrl, playbackJWT });
    const audioStream = ffmpegProcess.stdout;

    const transcriptionResponse = await setupTranscription({
      transcribeClient,
      audioStream,
      languageCode: fromLang,
    });

    if (!transcriptionResponse.TranscriptResultStream) {
      throw new Error("Failed to get a valid transcription response stream.");
    }

    for await (const event of transcriptionResponse.TranscriptResultStream) {
      const results = event.TranscriptEvent?.Transcript?.Results;
      if (
        results &&
        results.length > 0 &&
        results[0].Alternatives &&
        results[0].Alternatives.length > 0
      ) {
        const firstResult = results[0];
        const firstAlternative =
          firstResult.Alternatives && firstResult.Alternatives[0]
            ? firstResult.Alternatives[0]
            : null;
        if (firstAlternative) {
          const transcript = firstAlternative.Transcript;
          if (transcript) {
            sendTranscriptEvent({
              ivsChatClient,
              roomArn: ivsChatRoomArn,
              transcript,
              firstResult,
              languageCode: fromLang,
            });
            if (!firstResult.IsPartial) {
              saveTranscriptToDB({
                transcript,
                languageCode: fromLang,
                apiToken: laravelAPIToken,
                apiUrl: laravelAPIEndpoint,
              });
            }

            if (needsTranslation && translateClient) {
              handleTranslation({
                ivsChatClient,
                ivsChatRoomArn,
                translateClient,
                transcript,
                fromLang,
                toLang,
                firstResult,
                apiToken: laravelAPIToken,
                apiUrl: laravelAPIEndpoint,
              });
            }
          }
        }
      }
    }
    await cleanup();
  } catch (error) {
    console.error("A critical error occurred in the main process:", error);
    await cleanup();
    process.exit(1);
  }
};

main();
