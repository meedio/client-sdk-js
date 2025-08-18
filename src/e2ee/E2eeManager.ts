import { Encryption_Type, TrackInfo } from '@livekit/protocol';
import { EventEmitter } from 'events';
import type TypedEventEmitter from 'typed-emitter';
import log, { LogLevel, workerLogger } from '../logger';
import type RTCEngine from '../room/RTCEngine';
import type Room from '../room/Room';
import { ConnectionState } from '../room/Room';
import { DeviceUnsupportedError } from '../room/errors';
import { EngineEvent, ParticipantEvent, RoomEvent } from '../room/events';
import type RemoteTrack from '../room/track/RemoteTrack';
import type { Track } from '../room/track/Track';
import type { VideoCodec } from '../room/track/options';
import { mimeTypeToVideoCodecString } from '../room/track/utils';
import { isLocalTrack } from '../room/utils';
import type { BaseKeyProvider } from './KeyProvider';
import { E2EE_FLAG, E2EE_LOG_PREFIX } from './constants';
import { type E2EEManagerCallbacks, EncryptionEvent, KeyProviderEvent } from './events';
import type {
  E2EEManagerOptions,
  E2EEWorkerMessage,
  EnableMessage,
  EncodeMessage,
  InitMessage,
  KeyInfo,
  RTPVideoMapMessage,
  RatchetRequestMessage,
  RemoveTransformMessage,
  ScriptTransformOptions,
  SetKeyMessage,
  SifTrailerMessage,
  UpdateCodecMessage,
} from './types';
import { generateLogSessionId, isE2EESupported, isScriptTransformSupported } from './utils';

export interface BaseE2EEManager {
  setup(room: Room): void;
  setupEngine(engine: RTCEngine): void;
  setParticipantCryptorEnabled(enabled: boolean, participantIdentity: string): void;
  setSifTrailer(trailer: Uint8Array): void;
  on<E extends keyof E2EEManagerCallbacks>(event: E, listener: E2EEManagerCallbacks[E]): this;
}

/**
 * @experimental
 */
export class E2EEManager
  extends (EventEmitter as new () => TypedEventEmitter<E2EEManagerCallbacks>)
  implements BaseE2EEManager
{
  protected worker: Worker;

  protected room?: Room;

  private encryptionEnabled: boolean;

  private keyProvider: BaseKeyProvider;

  private localParticipantLogSessionId: string;

  constructor(options: E2EEManagerOptions) {
    super();
    this.keyProvider = options.keyProvider;
    this.worker = options.worker;
    this.encryptionEnabled = false;
    this.localParticipantLogSessionId = generateLogSessionId();
    log.info(`${E2EE_LOG_PREFIX} Your livekit logSessionId: ${this.localParticipantLogSessionId}`);
  }

  private get logContext() {
    return {
      localParticipant: this.room?.localParticipant.identity,
      logSessionId: this.localParticipantLogSessionId,
    };
  }

  /**
   * @internal
   */
  setup(room: Room) {
    if (!isE2EESupported()) {
      log.error(
        `${E2EE_LOG_PREFIX} tried to setup end-to-end encryption on an unsupported browser.`,
      );

      throw new DeviceUnsupportedError(
        'tried to setup end-to-end encryption on an unsupported browser',
      );
    }

    log.info(`${E2EE_LOG_PREFIX} setting up e2ee`, this.logContext);

    if (room !== this.room) {
      this.room = room;
      this.setupEventListeners(room, this.keyProvider);
      // this.worker = new Worker('');
      const msg: InitMessage = {
        kind: 'init',
        data: {
          keyProviderOptions: this.keyProvider.getOptions(),
          loglevel: workerLogger.getLevel() as LogLevel,
          logSessionId: this.localParticipantLogSessionId,
        },
      };
      if (this.worker) {
        log.info(`${E2EE_LOG_PREFIX} initializing worker`, {
          ...this.logContext,
          worker: this.worker,
        });
        this.worker.onmessage = this.onWorkerMessage;
        this.worker.onerror = this.onWorkerError;
        this.worker.postMessage(msg);
      } else {
        log.error(`${E2EE_LOG_PREFIX} worker is missing in e2ee setup`, this.logContext);
      }
    } else {
      log.error(`${E2EE_LOG_PREFIX} skipping e2ee setup. Room already exists`, this.logContext);
    }
  }

  /**
   * @internal
   */
  setParticipantCryptorEnabled(enabled: boolean, participantIdentity: string) {
    log.info(
      `${E2EE_LOG_PREFIX} posting to enable e2ee - ${enabled} for participant ${participantIdentity}`,
      { ...this.logContext, participantIdentity },
    );
    this.postEnable(enabled, participantIdentity);
  }

  /**
   * @internal
   */
  setSifTrailer(trailer: Uint8Array) {
    if (!trailer || trailer.length === 0) {
      log.warn(`${E2EE_LOG_PREFIX} ignoring server sent trailer as it's empty`, {
        ...this.logContext,
        localParticipantIdentity: this.room?.localParticipant.identity,
      });
    } else {
      this.postSifTrailer(trailer);
    }
  }

  private onWorkerMessage = (ev: MessageEvent<E2EEWorkerMessage>) => {
    const { kind, data } = ev.data;
    switch (kind) {
      case 'error':
        log.error(`${E2EE_LOG_PREFIX} received an error message from the worker`, {
          ...this.logContext,
          errorMessage: data.error.message,
        });
        log.error(data.error.message);
        this.emit(EncryptionEvent.EncryptionError, data.error);
        break;
      case 'initAck':
        log.info(`${E2EE_LOG_PREFIX} received "initAck" message from the worker`, this.logContext);
        if (data.enabled) {
          log.info(
            `${E2EE_LOG_PREFIX} will post ${this.keyProvider.getKeys().length} initial keys`,
            { ...this.logContext, e2eeEnabled: data.enabled },
          );

          this.keyProvider.getKeys().forEach((keyInfo) => {
            this.postKey(keyInfo);
          });
        }
        break;

      case 'enable':
        log.info(`${E2EE_LOG_PREFIX} received "enable" message from the worker`, {
          ...this.logContext,
          e2eeEnabled: data.enabled,
          participantIdentity: data.participantIdentity,
        });

        if (data.enabled) {
          log.info(`${E2EE_LOG_PREFIX} will post ${this.keyProvider.getKeys().length} keys`, {
            ...this.logContext,
            participantIdentity: data.participantIdentity,
          });
          this.keyProvider.getKeys().forEach((keyInfo) => {
            this.postKey(keyInfo);
          });
        }
        if (
          this.encryptionEnabled !== data.enabled &&
          data.participantIdentity === this.room?.localParticipant.identity
        ) {
          log.info(
            `${E2EE_LOG_PREFIX} participant encryption status changed for the local participant, emitting event`,
            { ...this.logContext, e2eeEnabled: data.enabled },
          );

          this.emit(
            EncryptionEvent.ParticipantEncryptionStatusChanged,
            data.enabled,
            this.room!.localParticipant,
          );
          this.encryptionEnabled = data.enabled;
        } else if (data.participantIdentity) {
          const participant = this.room?.getParticipantByIdentity(data.participantIdentity);
          if (!participant) {
            log.error(`${E2EE_LOG_PREFIX} couldn't set encryption status, participant not found`, {
              ...this.logContext,
              e2eeEnabled: data.enabled,
              participantIdentity: data.participantIdentity,
            });
            throw TypeError(
              `couldn't set encryption status, participant not found${data.participantIdentity}`,
            );
          }

          log.info(
            `${E2EE_LOG_PREFIX} participant encryption status changed for a participant, emitting event`,
            {
              ...this.logContext,
              e2eeEnabled: data.enabled,
              participantIdentity: data.participantIdentity,
            },
          );

          this.emit(EncryptionEvent.ParticipantEncryptionStatusChanged, data.enabled, participant);
        }
        break;
      case 'ratchetKey':
        log.info(
          `${E2EE_LOG_PREFIX} received "ratchetKey" message from the worker. Emitting KeyProviderEvent.KeyRatcheted event`,
          {
            ...this.logContext,
            participantIdentity: data.participantIdentity,
            keyIndex: data.keyIndex,
            ratchetResult: data.ratchetResult,
          },
        );
        this.keyProvider.emit(
          KeyProviderEvent.KeyRatcheted,
          data.ratchetResult,
          data.participantIdentity,
          data.keyIndex,
        );
        break;
      case 'logging':
        log.info(data.message, data.properties);
        break;
      default:
        break;
    }
  };

  private onWorkerError = (ev: ErrorEvent) => {
    log.error(`${E2EE_LOG_PREFIX} e2ee worker encountered an error`, {
      ...this.logContext,
      error: ev,
    });

    this.emit(EncryptionEvent.EncryptionError, ev.error);
  };

  public setupEngine(engine: RTCEngine) {
    engine.on(EngineEvent.RTPVideoMapUpdate, (rtpMap) => {
      this.postRTPMap(rtpMap);
    });
  }

  private setupEventListeners(room: Room, keyProvider: BaseKeyProvider) {
    room.on(RoomEvent.TrackPublished, (pub, participant) =>
      this.setParticipantCryptorEnabled(
        pub.trackInfo!.encryption !== Encryption_Type.NONE,
        participant.identity,
      ),
    );
    room
      .on(RoomEvent.ConnectionStateChanged, (state) => {
        if (state === ConnectionState.Connected) {
          room.remoteParticipants.forEach((participant) => {
            participant.trackPublications.forEach((pub) => {
              this.setParticipantCryptorEnabled(
                pub.trackInfo!.encryption !== Encryption_Type.NONE,
                participant.identity,
              );
            });
          });
        }
      })
      .on(RoomEvent.TrackUnsubscribed, (track, _, participant) => {
        const msg: RemoveTransformMessage = {
          kind: 'removeTransform',
          data: {
            participantIdentity: participant.identity,
            trackId: track.mediaStreamID,
          },
        };
        this.worker?.postMessage(msg);
      })
      .on(RoomEvent.TrackSubscribed, (track, pub, participant) => {
        this.setupE2EEReceiver(track, participant.identity, pub.trackInfo);
      })
      .on(RoomEvent.SignalConnected, () => {
        if (!this.room) {
          log.error(
            `${E2EE_LOG_PREFIX} room is missing, expected room to be present on signal connect`,
            this.logContext,
          );
          throw new TypeError(`expected room to be present on signal connect`);
        }

        log.info(
          `${E2EE_LOG_PREFIX} signal connected event received, will post ${keyProvider.getKeys().length} keys`,
          this.logContext,
        );
        keyProvider.getKeys().forEach((keyInfo) => {
          this.postKey(keyInfo);
        });
        this.setParticipantCryptorEnabled(
          this.room.localParticipant.isE2EEEnabled,
          this.room.localParticipant.identity,
        );
      });

    room.localParticipant.on(ParticipantEvent.LocalSenderCreated, async (sender, track) => {
      this.setupE2EESender(track, sender);
    });

    keyProvider
      .on(KeyProviderEvent.SetKey, (keyInfo) => this.postKey(keyInfo))
      .on(KeyProviderEvent.RatchetRequest, (participantId, keyIndex) =>
        this.postRatchetRequest(participantId, keyIndex),
      );
  }

  private postRatchetRequest(participantIdentity?: string, keyIndex?: number) {
    if (!this.worker) {
      log.error(`${E2EE_LOG_PREFIX} could not ratchet key, worker is missing`, {
        ...this.logContext,
        participantIdentity,
        keyIndex,
      });
      throw Error('could not ratchet key, worker is missing');
    }

    log.info(`${E2EE_LOG_PREFIX} posting "ratchetRequest"`, {
      ...this.logContext,
      participantIdentity,
      keyIndex,
    });

    const msg: RatchetRequestMessage = {
      kind: 'ratchetRequest',
      data: {
        participantIdentity: participantIdentity,
        keyIndex,
      },
    };
    this.worker.postMessage(msg);
  }

  private postKey({ key, participantIdentity, keyIndex }: KeyInfo) {
    if (!this.worker) {
      log.error(`${E2EE_LOG_PREFIX} could not post key, worker is missing`, {
        ...this.logContext,
        participantIdentity,
        keyIndex,
      });
      throw Error('could not set key, worker is missing');
    }

    log.info(`${E2EE_LOG_PREFIX} posting "setKey"`, {
      ...this.logContext,
      participantIdentity,
      keyIndex,
    });

    const msg: SetKeyMessage = {
      kind: 'setKey',
      data: {
        participantIdentity: participantIdentity,
        isPublisher: participantIdentity === this.room?.localParticipant.identity,
        key,
        keyIndex,
      },
    };
    this.worker.postMessage(msg);
  }

  private postEnable(enabled: boolean, participantIdentity: string) {
    if (this.worker) {
      log.info(`${E2EE_LOG_PREFIX} posting "enable"`, { ...this.logContext, participantIdentity });
      const enableMsg: EnableMessage = {
        kind: 'enable',
        data: {
          enabled,
          participantIdentity,
        },
      };
      this.worker.postMessage(enableMsg);
    } else {
      log.error(`${E2EE_LOG_PREFIX} could not post "enable", worker is not ready or missing`, {
        ...this.logContext,
        participantIdentity,
      });
      throw new ReferenceError('failed to enable e2ee, worker is not ready');
    }
  }

  private postRTPMap(map: Map<number, VideoCodec>) {
    if (!this.worker) {
      log.error(`${E2EE_LOG_PREFIX} could not post rtp map, worker is missing.`, this.logContext);

      throw TypeError('could not post rtp map, worker is missing');
    }
    if (!this.room?.localParticipant.identity) {
      log.error(
        `${E2EE_LOG_PREFIX} could not post rtp map, local participant identity is missing`,
        this.logContext,
      );
      throw TypeError('could not post rtp map, local participant identity is missing');
    }

    log.info(`${E2EE_LOG_PREFIX} posting "setRTPMap"`, this.logContext);

    const msg: RTPVideoMapMessage = {
      kind: 'setRTPMap',
      data: {
        map,
        participantIdentity: this.room.localParticipant.identity,
      },
    };
    this.worker.postMessage(msg);
  }

  private postSifTrailer(trailer: Uint8Array) {
    if (!this.worker) {
      log.error(`${E2EE_LOG_PREFIX} could not post SIF trailer, worker is missing`, {
        ...this.logContext,
        trailer,
      });
      throw Error('could not post SIF trailer, worker is missing');
    }

    log.info(`${E2EE_LOG_PREFIX} posting "setSifTrailer"`, { ...this.logContext, trailer });

    const msg: SifTrailerMessage = {
      kind: 'setSifTrailer',
      data: {
        trailer,
      },
    };
    this.worker.postMessage(msg);
  }

  private setupE2EEReceiver(track: RemoteTrack, remoteId: string, trackInfo?: TrackInfo) {
    log.info(`${E2EE_LOG_PREFIX} started setting up e2ee receiver for ${track.source}`, {
      ...this.logContext,
      track,
      trackInfo,
      participantIdentity: remoteId,
    });

    if (!track.receiver) {
      log.error(`${E2EE_LOG_PREFIX} failed to setup e2ee receiver, track.receiver ir missing`, {
        ...this.logContext,
        track,
        trackInfo,
        participantIdentity: remoteId,
      });
      return;
    }
    if (!trackInfo?.mimeType || trackInfo.mimeType === '') {
      log.error(
        `${E2EE_LOG_PREFIX} failed to setup e2ee receiver, mimeType missing from trackInfo, cannot set up E2EE cryptor`,
        { ...this.logContext, track, trackInfo, participantIdentity: remoteId },
      );
      throw new TypeError('MimeType missing from trackInfo, cannot set up E2EE cryptor');
    }

    this.handleReceiver(
      track.receiver,
      track.mediaStreamID,
      remoteId,
      track.kind === 'video' ? mimeTypeToVideoCodecString(trackInfo.mimeType) : undefined,
    );
  }

  private setupE2EESender(track: Track, sender: RTCRtpSender) {
    log.info(`${E2EE_LOG_PREFIX} started setting up e2ee sender for ${track.source}`, {
      ...this.logContext,
      track,
    });

    if (!isLocalTrack(track) || !sender) {
      log.error(`${E2EE_LOG_PREFIX} failed to setup e2ee sender`, {
        ...this.logContext,
        track,
        sender: sender,
      });
      if (!sender) log.warn('early return because sender is not ready');
      return;
    }
    this.handleSender(sender, track.mediaStreamID, undefined);
  }

  /**
   * Handles the given {@code RTCRtpReceiver} by creating a {@code TransformStream} which will inject
   * a frame decoder.
   *
   */
  private async handleReceiver(
    receiver: RTCRtpReceiver,
    trackId: string,
    participantIdentity: string,
    codec?: VideoCodec,
  ) {
    if (!this.worker) {
      log.error(`${E2EE_LOG_PREFIX} failed to setup receiver, worker is missing`, {
        ...this.logContext,
        trackId,
        participantIdentity,
      });
      return;
    }

    if (isScriptTransformSupported()) {
      const options: ScriptTransformOptions = {
        kind: 'decode',
        participantIdentity,
        trackId,
        codec,
      };

      log.info(
        `${E2EE_LOG_PREFIX} setting up receiver, isScriptTransformSupported === true, creating a new RTCRtpScriptTransform`,
        {
          ...this.logContext,
          ...options,
        },
      );

      // @ts-ignore
      receiver.transform = new RTCRtpScriptTransform(this.worker, options);
    } else {
      if (E2EE_FLAG in receiver && codec) {
        // only update codec
        const msg: UpdateCodecMessage = {
          kind: 'updateCodec',
          data: {
            trackId,
            codec,
            participantIdentity: participantIdentity,
          },
        };

        log.warn(
          `${E2EE_LOG_PREFIX} setting up receiver, isScriptTransformSupported === false, posting "updateCodec" message and returning`,
          { ...this.logContext, trackId, codec, participantIdentity },
        );

        this.worker.postMessage(msg);
        return;
      }
      // @ts-ignore
      let writable: WritableStream = receiver.writableStream;
      // @ts-ignore
      let readable: ReadableStream = receiver.readableStream;

      if (!writable || !readable) {
        // @ts-ignore
        const receiverStreams = receiver.createEncodedStreams();
        // @ts-ignore
        receiver.writableStream = receiverStreams.writable;
        writable = receiverStreams.writable;
        // @ts-ignore
        receiver.readableStream = receiverStreams.readable;
        readable = receiverStreams.readable;
      }

      const msg: EncodeMessage = {
        kind: 'decode',
        data: {
          readableStream: readable,
          writableStream: writable,
          trackId: trackId,
          codec,
          participantIdentity: participantIdentity,
          isReuse: E2EE_FLAG in receiver,
        },
      };

      log.info(`${E2EE_LOG_PREFIX} initializing decoded streams, posting "decode"`, {
        ...this.logContext,
        trackId,
        codec,
        isReuse: E2EE_FLAG in receiver,
        participantIdentity: participantIdentity,
      });

      this.worker.postMessage(msg, [readable, writable]);
    }

    // @ts-ignore
    receiver[E2EE_FLAG] = true;
  }

  /**
   * Handles the given {@code RTCRtpSender} by creating a {@code TransformStream} which will inject
   * a frame encoder.
   *
   */
  private handleSender(sender: RTCRtpSender, trackId: string, codec?: VideoCodec) {
    if (E2EE_FLAG in sender || !this.worker) {
      log.error(`${E2EE_LOG_PREFIX} failed to handle sender. E2EE flag or worker is missing`, {
        ...this.logContext,
        trackId,
        codec,
        hasE2eeFlag: E2EE_FLAG in sender,
        hasWorker: !!this.worker,
        sender,
      });
      return;
    }

    if (!this.room?.localParticipant.identity || this.room.localParticipant.identity === '') {
      log.error(
        `${E2EE_LOG_PREFIX} local identity needs to be known in order to set up encrypted sender`,
        { ...this.logContext, trackId, codec, sender },
      );
      throw TypeError('local identity needs to be known in order to set up encrypted sender');
    }

    if (isScriptTransformSupported()) {
      const options = {
        kind: 'encode',
        participantIdentity: this.room.localParticipant.identity,
        trackId,
        codec,
      };

      log.info(`${E2EE_LOG_PREFIX} setting up sender, isScriptTransformSupported === true`, {
        ...this.logContext,
        kind: 'encode',
        trackId,
        codec,
        sender,
      });

      // @ts-ignore
      sender.transform = new RTCRtpScriptTransform(this.worker, options);
    } else {
      // @ts-ignore
      const senderStreams = sender.createEncodedStreams();
      const msg: EncodeMessage = {
        kind: 'encode',
        data: {
          readableStream: senderStreams.readable,
          writableStream: senderStreams.writable,
          codec,
          trackId,
          participantIdentity: this.room.localParticipant.identity,
          isReuse: false,
        },
      };

      log.info(
        `${E2EE_LOG_PREFIX} initializing encoded streams, isScriptTransformSupported === false, posting "encode" message`,
        { ...this.logContext, trackId, codec, sender, isReuse: false },
      );
      this.worker.postMessage(msg, [senderStreams.readable, senderStreams.writable]);
    }

    // @ts-ignore
    sender[E2EE_FLAG] = true;
  }
}
