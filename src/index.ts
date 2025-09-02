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
  domain,
  livestreamID,
  startTime,
  endTime,
  taskStartTime,
}: {
  transcript: string;
  languageCode: string;
  domain: string;
  livestreamID: string;
  startTime: number | undefined;
  endTime: number | undefined;
  taskStartTime: number;
}) => {
  try {
    await axios.post(
      `https://${domain}/api/livestreams-v2/${livestreamID}/rtmps-transcription`,
      {
        transcript,
        language_code: languageCode,
        start_time: startTime ?? Date.now() - taskStartTime,
        end_time: endTime,
      },
      {
        headers: {
          // "X-Xsrf-Token": test,
          Accept: "application/json",
          Cookie:
            // testing
            // "remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d=eyJpdiI6ImF1NVBhRjNIaTNYRzdyMFZ0MC9yalE9PSIsInZhbHVlIjoiOGJOMXBZaEVLTHlhbi9udVdJNnpFUFRTeGMyLzV3TUsrQ2ZySTVJbzlMb3J2V0hGMythWTRxWTdXRUdpaVJKWWx6blQ4R2VUcHBYZ3ZGcmQycHJYcWtiSFNRdE5nalJMTHVlWjZ3WDRIb1dPZVNXZU1vOEZSbXcvNkFqY0gvOTVCQ0tlN3hIRlhQK25uVGtocCtkWW9iUmt2Z08yckZUNDg4K2dTd3duays2b0paODZIeUg3Ykp0WTBNV2kxR21nbzhEbXY0bkx4cmUybDBzcDNpTHJ4dG1jeCtjM211NE5Hb3haWm9EeGhVND0iLCJtYWMiOiJjMGZmZTM5NTM2NTdhMTQ3ODNlODFhZjViNzA3MTdjY2UxNmM5ZTEzNjEyYTQxNjVlMTk5YTM4MDAyOGQyNzMyIiwidGFnIjoiIn0%3D; laravel_token=eyJpdiI6Im45d2pCODN4a0EyWFB0MVlCWENEbXc9PSIsInZhbHVlIjoiSVZKaVNSRFlmNEkvSDJTQWJTQzJWWVJhZzVUMEZuYS9WWGZUMzUzcktxZ3BXV1V5dW9LMDJDdnZEKy9TbDlLVm9iaitlUEpNUUZ2d1ZTVFJhanJCek91dmpkeG5QNkc1TlhzVHc3bWlOYTZST1VUQkhLL0lJQmpieG5qbXA1WUtKcVhaUVVtT3lRMkxuOHRZKytVcjV0QnBXOHUrTlY3ZXByQlRvMHNrS1pBTG9QT25MMFdMVlU5c08vSDhObUhxNE5uUEFrdis3dllzSy9LVWhTUXJ3QVBQUndncUE3NU9WUzI0V1ZDcUhia2RWYjkwMDNlMDBRRWZHMDY5QjhDTTA3S0Q0ZFpTUCtWRFNkd2FCQW5BcjdrOTJNMXErQjRHallvSW0yL2ZlY3lGaEVhcHlXZ2tCazdPMWZON3hpRkJrMEt3NlRNTmpOeExiemtHcW84eHEyNkdRVVNqQUdJY1lSMWZBU3VwamdiakhuYTFQckhqTzlFMUZUYkl0dk9SV21ad204M09yVS94UEg2MGc2Qlh1YnNqbjkyYlE5WGF0cWt2aGVGVU9pSFUrSGVHUGw2KzVEMkdCMXJIZkxBdGllbEkvcklQaHdJTndDd1B4MmhoZHJ5ZktWR0pUanVmd3BLZG0wWS80ckdldlkvdlNqM2I3RUNwV29hQ1VTTHZvZE5RQVBnMkFFS3MvK3pqMk1iK1dQdDVGbVZyeVRlb1o3dEl5MmhpdDNlRVd1aVBReUxiOXNXSHlXVFd4WUpUNXczRTdQUFYwSmllNStJemdlb1c0UUFMb3NoZ1RRd3FOWmxweTJGWnpieG5ZRnhPSUVtY1RhbVJtTWpsd3RGTGRhcXMwK1NKZWxnTjhzVnYwK1YxWVA4TkEzamt1M0NtTFROUi9qQTVnWmRBMjVOSVc2aE45ZW5IZGNFbG81NFEycUdiU0VzN05uRVZEcTFIdEpWN1VQc0l3Z2I3dDY4b0d3RnJnakg4UUNtTjYydmI0Vnp0THdRdTV3R1Y1OFU5WFdQcXhnSFIyMXhSRnkxUVR1cldJQT09IiwibWFjIjoiOWMxMDk2NDQyNmE3OTQxZmQ0ZjhkZWI0YTRlMzVmMDNhNGFhYzdkNzNhNjcyMjI2MjAwNWU3NmMwNmQ4NThhYiIsInRhZyI6IiJ9; XSRF-TOKEN=eyJpdiI6IjlUajhnL2dFMStMendiQlBUZ1VBMlE9PSIsInZhbHVlIjoieVA0UHBQNGliTXcrdTBnSDhzL1lFRk1EQlU1NmtRRWVvRk0ydW9BVVh6M3U5aHVlSFVkOS9GYXJWaUFFRGtYZE1ac2ZlbFAvVzFnUnBudDJjYU05ckM2WXRpWkRYODl0aGg5QzFEYi9ERXg5Z0FadXhRVmZXbTNOQ0VRTjBXeGUiLCJtYWMiOiJkNzkyOTVkYjAzYTNiZDQzNTk4MGUyNGJkYjI1MmY2MzBiNjM3ZmJhMDM3NDk4YjY4MDk1M2YxYmQyYWE3YzBjIiwidGFnIjoiIn0%3D; workvivo_session=eyJpdiI6ImFmZWtmamlzSTdXamt5dVRTY21SOVE9PSIsInZhbHVlIjoieUJTcEFHd1RKMW5uNjFBVkJpdnRrUHRCSmNqTmNMTWJLaWkyOU9QbHYzcWVqU3VINENlZGRqSkN4eVNHTXArbzRuQkZwRExTcGJ4OUZzdC9pQWdMemVHSGtyaVMzVWgxeEI1MEN0UEJpUTFqTzhiQXByTnMzUGdJbU1ySGVQZ1EiLCJtYWMiOiIwYjE0MTRmMzQwOTdiZTdmZGI3NDAxZWVkZWI0MDc0YzgxN2IyN2NmMTY0M2Q0ZGZjZTZmZWU1ZWI1ZDQwMTJhIiwidGFnIjoiIn0%3D; x-clockwork=%7B%22requestId%22%3A%221756740918-7397-530711230%22%2C%22version%22%3A%225.3.1%22%2C%22path%22%3A%22%5C%2F__clockwork%5C%2F%22%2C%22webPath%22%3A%22%5C%2Fclockwork%5C%2Fapp%22%2C%22token%22%3A%2253b06eee%22%2C%22metrics%22%3Atrue%2C%22toolbar%22%3Atrue%7D",
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
  domain,
  livestreamID,
  taskStartTime,
}: {
  ivsChatClient: IvschatClient;
  ivsChatRoomArn: string;
  translateClient: TranslateClient;
  transcript: string;
  fromLang: LanguageCode;
  toLang: LanguageCode;
  firstResult: Result;
  domain: string;
  livestreamID: string;
  taskStartTime: number;
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
      transcript: translatedText,
      languageCode: toLang,
      domain,
      livestreamID,
      startTime: firstResult.StartTime,
      endTime: firstResult.EndTime,
      taskStartTime,
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
  const taskStartTime = Date.now();

  const playbackUrl: EnvVar = process.env.PLAYBACK_URL;
  const awsIVSRegion: EnvVar = process.env.AWS_IVS_REGION;
  const awsTranscribeRegion: EnvVar = process.env.AWS_TRANSCRIBE_REGION;
  const awsTranslateRegion: EnvVar = process.env.AWS_TRANSLATE_REGION;
  const ivsChatRoomArn: EnvVar = process.env.IVS_CHAT_ROOM_ARN;
  const livestreamID: EnvVar = process.env.LIVESTREAM_ID;
  const playbackJWT: EnvVar = process.env.PLAYBACK_JWT;
  const fromLang: LanguageEnvVar =
    (process.env.FROM_LANG as LanguageCode) || "en-IE";
  const toLang: LanguageEnvVar =
    (process.env.TO_LANG as LanguageCode) || "en-IE";
  const domain: EnvVar = process.env.DOMAIN;

  if (
    !playbackUrl ||
    !ivsChatRoomArn ||
    !awsIVSRegion ||
    !awsTranscribeRegion ||
    !awsTranslateRegion ||
    !playbackJWT ||
    !domain ||
    !livestreamID
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
    ivsChatClient = new IvschatClient({ region: awsIVSRegion });
  } catch (err) {
    console.error("Failed to instantiate IVS Chat client:", err);
    await cleanup();
    process.exit(1);
  }

  // Init Transcribe Client
  try {
    transcribeClient = new TranscribeStreamingClient({
      region: awsTranscribeRegion,
    });
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
      translateClient = new TranslateClient({ region: awsTranslateRegion });
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
                domain,
                livestreamID,
                startTime: firstResult.StartTime,
                endTime: firstResult.EndTime,
                taskStartTime,
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
                domain,
                livestreamID,
                taskStartTime,
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
