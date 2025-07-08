import { workerLogger } from '../../logger';
import type { VideoCodec } from '../../room/track/options';
import { AsyncQueue } from '../../utils/AsyncQueue';
import { E2EE_LOG_PREFIX, KEY_PROVIDER_DEFAULTS } from '../constants';
import { CryptorErrorReason } from '../errors';
import { CryptorEvent, KeyHandlerEvent } from '../events';
import type {
  E2EEWorkerMessage,
  ErrorMessage,
  InitAck,
  KeyProviderOptions,
  RatchetMessage,
  RatchetRequestMessage,
  RatchetResult,
  ScriptTransformOptions,
} from '../types';
import { FrameCryptor, encryptionEnabledMap } from './FrameCryptor';
import { ParticipantKeyHandler } from './ParticipantKeyHandler';

const E2EE_WORKER_LOG_PREFIX = E2EE_LOG_PREFIX + '[worker]';
const participantCryptors: FrameCryptor[] = [];
const participantKeys: Map<string, ParticipantKeyHandler> = new Map();
let sharedKeyHandler: ParticipantKeyHandler | undefined;
let messageQueue = new AsyncQueue();

let isEncryptionEnabled: boolean = false;

let useSharedKey: boolean = false;

let sifTrailer: Uint8Array | undefined;

let keyProviderOptions: KeyProviderOptions = KEY_PROVIDER_DEFAULTS;

let rtpMap: Map<number, VideoCodec> = new Map();

let logSessionId: string | null = null;

const getLogContext = () => ({ logSessionId });

workerLogger.setDefaultLevel('info');

onmessage = (ev) => {
  messageQueue.run(async () => {
    const { kind, data }: E2EEWorkerMessage = ev.data;

    switch (kind) {
      case 'init':
        workerLogger.setLevel(data.loglevel);
        workerLogger.info(
          `${E2EE_WORKER_LOG_PREFIX} worker initialized. Posting acknowledgement`,
          getLogContext(),
        );
        keyProviderOptions = data.keyProviderOptions;
        useSharedKey = !!data.keyProviderOptions.sharedKey;
        logSessionId = data.logSessionId;
        // acknowledge init successful
        const ackMsg: InitAck = {
          kind: 'initAck',
          data: { enabled: isEncryptionEnabled },
        };
        postMessage(ackMsg);
        break;
      case 'enable':
        setEncryptionEnabled(data.enabled, data.participantIdentity);
        workerLogger.info(
          `${E2EE_WORKER_LOG_PREFIX} updated e2ee enabled status for ${data.participantIdentity} to ${data.enabled}`,
          getLogContext(),
        );
        // acknowledge enable call successful
        postMessage(ev.data);
        break;
      case 'decode':
        let cryptor = getTrackCryptor(data.participantIdentity, data.trackId);
        cryptor.setupTransform(
          kind,
          data.readableStream,
          data.writableStream,
          data.trackId,
          data.isReuse,
          data.codec,
        );

        workerLogger.info(
          `${E2EE_WORKER_LOG_PREFIX} received "decode". Running setupTransform on cryptor`,
          {
            ...getLogContext(),
            participantIdentity: data.participantIdentity,
            trackId: data.trackId,
          },
        );
        break;
      case 'encode':
        let pubCryptor = getTrackCryptor(data.participantIdentity, data.trackId);
        pubCryptor.setupTransform(
          kind,
          data.readableStream,
          data.writableStream,
          data.trackId,
          data.isReuse,
          data.codec,
        );

        workerLogger.info(
          `${E2EE_WORKER_LOG_PREFIX} received "encode". Running setupTransform on pubCryptor`,
          {
            ...getLogContext(),
            participantIdentity: data.participantIdentity,
            trackId: data.trackId,
          },
        );
        break;
      case 'setKey':
        workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} received "setKey"`, {
          ...getLogContext(),
          participantIdentity: data.participantIdentity,
          keyIndex: data.keyIndex,
          useSharedKey,
        });
        if (useSharedKey) {
          await setSharedKey(data.key, data.keyIndex);
          workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} successfully set shared key`, {
            ...getLogContext(),
            participantIdentity: data.participantIdentity,
            keyIndex: data.keyIndex,
          });
        } else if (data.participantIdentity) {
          workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} will set key on participant key handler`, {
            ...getLogContext(),
            participantIdentity: data.participantIdentity,
            keyIndex: data.keyIndex,
          });
          await getParticipantKeyHandler(data.participantIdentity).setKey(data.key, data.keyIndex);
          workerLogger.info(
            `${E2EE_WORKER_LOG_PREFIX} successfully set key on participant key handler`,
            {
              ...getLogContext(),
              participantIdentity: data.participantIdentity,
              keyIndex: data.keyIndex,
            },
          );
        } else {
          workerLogger.error(
            `${E2EE_WORKER_LOG_PREFIX} no participant Id was provided and shared key usage is disabled`,
            {
              ...getLogContext(),
              participantIdentity: data.participantIdentity,
              keyIndex: data.keyIndex,
            },
          );
        }
        break;
      case 'removeTransform':
        workerLogger.info(
          `${E2EE_WORKER_LOG_PREFIX} received "removeTransform", will unset cryptor for participant`,
          {
            ...getLogContext(),
            participantIdentity: data.participantIdentity,
            trackId: data.trackId,
          },
        );
        unsetCryptorParticipant(data.trackId, data.participantIdentity);
        break;
      case 'updateCodec':
        workerLogger.info(
          `${E2EE_WORKER_LOG_PREFIX} received "updateCodec", will set a video codec for track cryptor`,
          {
            ...getLogContext(),
            participantIdentity: data.participantIdentity,
            trackId: data.trackId,
            newCodec: data.codec,
          },
        );
        getTrackCryptor(data.participantIdentity, data.trackId).setVideoCodec(data.codec);
        break;
      case 'setRTPMap':
        workerLogger.info(
          `${E2EE_WORKER_LOG_PREFIX} received "setRTPMap", will set a rtp map for cryptors`,
          { ...getLogContext(), participantIdentity: data.participantIdentity },
        );
        // this is only used for the local participant
        rtpMap = data.map;
        participantCryptors.forEach((cr) => {
          if (cr.getParticipantIdentity() === data.participantIdentity) {
            cr.setRtpMap(data.map);
          }
        });
        break;
      case 'ratchetRequest':
        workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} received "ratchetRequest"`, {
          ...getLogContext(),
          participantIdentity: data.participantIdentity,
        });
        handleRatchetRequest(data);
        break;
      case 'setSifTrailer':
        workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} received "setSifTrailer"`, getLogContext());
        handleSifTrailer(data.trailer);
        break;
      default:
        break;
    }
  });
};

async function handleRatchetRequest(data: RatchetRequestMessage['data']) {
  if (useSharedKey) {
    workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} handling ratchet request when using shared key`, {
      ...getLogContext(),
      participantIdentity: data.participantIdentity,
    });
    const keyHandler = getSharedKeyHandler();
    await keyHandler.ratchetKey(data.keyIndex);
    keyHandler.resetKeyStatus();
    workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} ratchet key successfully handled`, {
      ...getLogContext(),
      participantIdentity: data.participantIdentity,
    });
  } else if (data.participantIdentity) {
    workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} handling ratchet request without a shared key`, {
      ...getLogContext(),
      participantIdentity: data.participantIdentity,
    });
    const keyHandler = getParticipantKeyHandler(data.participantIdentity);
    await keyHandler.ratchetKey(data.keyIndex);
    keyHandler.resetKeyStatus();
    workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} ratchet key successfully handled`, {
      ...getLogContext(),
      participantIdentity: data.participantIdentity,
    });
  } else {
    workerLogger.error(
      `${E2EE_WORKER_LOG_PREFIX} no participant ID was provided for ratchet request and shared key usage is disabled`,
      { ...getLogContext(), participantIdentity: data.participantIdentity },
    );
  }
}

function getTrackCryptor(participantIdentity: string, trackId: string) {
  let cryptors = participantCryptors.filter((c) => c.getTrackId() === trackId);
  if (cryptors.length > 1) {
    const debugInfo = cryptors
      .map((c) => {
        return { participant: c.getParticipantIdentity() };
      })
      .join(',');
    workerLogger.error(`${E2EE_WORKER_LOG_PREFIX} found multiple cryptors for the same trackID`, {
      ...getLogContext(),
      trackId,
      participantIdentity,
      debugInfo,
    });
  }
  let cryptor = cryptors[0];
  if (!cryptor) {
    workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} creating new cryptor`, {
      ...getLogContext(),
      trackId,
      participantIdentity,
    });
    if (!keyProviderOptions) {
      workerLogger.error(
        `${E2EE_WORKER_LOG_PREFIX} tried to get the track cryptor, but missing keyProvider options`,
        { ...getLogContext(), trackId, participantIdentity },
      );
      throw Error('Missing keyProvider options');
    }
    cryptor = new FrameCryptor({
      participantIdentity,
      keys: getParticipantKeyHandler(participantIdentity),
      keyProviderOptions,
      sifTrailer,
      logSessionId,
    });
    cryptor.setRtpMap(rtpMap);
    setupCryptorErrorEvents(cryptor);
    participantCryptors.push(cryptor);
  } else if (participantIdentity !== cryptor.getParticipantIdentity()) {
    workerLogger.info(
      `${E2EE_WORKER_LOG_PREFIX} assigning a new participant id to track cryptor and passing in a correct key handler`,
      { ...getLogContext(), trackId, participantIdentity },
    );
    // assign new participant id to track cryptor and pass in correct key handler
    cryptor.setParticipant(participantIdentity, getParticipantKeyHandler(participantIdentity));
  }

  return cryptor;
}

function getParticipantKeyHandler(participantIdentity: string) {
  workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} getting participant key handler`, {
    ...getLogContext(),
    participantIdentity,
  });
  if (useSharedKey) {
    return getSharedKeyHandler();
  }
  let keys = participantKeys.get(participantIdentity);
  if (!keys) {
    workerLogger.warn(
      `${E2EE_WORKER_LOG_PREFIX} participant had no keys, creating new participantKeyHandler`,
      { ...getLogContext(), participantIdentity },
    );
    keys = new ParticipantKeyHandler(participantIdentity, keyProviderOptions, logSessionId);
    keys.on(KeyHandlerEvent.KeyRatcheted, emitRatchetedKeys);
    participantKeys.set(participantIdentity, keys);
  }

  return keys;
}

function getSharedKeyHandler() {
  workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} getting shared key handler`, getLogContext());
  if (!sharedKeyHandler) {
    workerLogger.warn(
      `${E2EE_WORKER_LOG_PREFIX} had no shared key handler, creating new shared key handler`,
      getLogContext(),
    );
    sharedKeyHandler = new ParticipantKeyHandler('shared-key', keyProviderOptions, logSessionId);
  }
  return sharedKeyHandler;
}

function unsetCryptorParticipant(trackId: string, participantIdentity: string) {
  const cryptors = participantCryptors.filter(
    (c) => c.getParticipantIdentity() === participantIdentity && c.getTrackId() === trackId,
  );
  if (cryptors.length > 1) {
    workerLogger.error(
      `${E2EE_WORKER_LOG_PREFIX} tried to unset a cryptor, but found multiple cryptors for the same participant and trackID combination`,
      { ...getLogContext(), trackId, participantIdentity },
    );
  }
  const cryptor = cryptors[0];
  if (!cryptor) {
    workerLogger.error(
      `${E2EE_WORKER_LOG_PREFIX} could not unset participant on cryptor, no cryptor was found for participant`,
      { ...getLogContext(), trackId, participantIdentity },
    );
  } else {
    cryptor.unsetParticipant();
    workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} successfully unset cryptor`, {
      ...getLogContext(),
      trackId,
      participantIdentity,
    });
  }
}

function setEncryptionEnabled(enable: boolean, participantIdentity: string) {
  workerLogger.info(
    `${E2EE_WORKER_LOG_PREFIX} setting encryption enabled for all tracks of ${participantIdentity}`,
    { ...getLogContext(), enable, participantIdentity },
  );
  encryptionEnabledMap.set(participantIdentity, enable);
}

async function setSharedKey(key: CryptoKey, index?: number) {
  // add logs inside participantKeyHandler
  await getSharedKeyHandler().setKey(key, index);
}

function setupCryptorErrorEvents(cryptor: FrameCryptor) {
  cryptor.on(CryptorEvent.Error, (error) => {
    const msg: ErrorMessage = {
      kind: 'error',
      data: { error: new Error(`${CryptorErrorReason[error.reason]}: ${error.message}`) },
    };
    postMessage(msg);
  });
}

function emitRatchetedKeys(
  ratchetResult: RatchetResult,
  participantIdentity: string,
  keyIndex?: number,
) {
  workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} emitting ratchetKey`, {
    ...getLogContext(),
    participantIdentity,
    keyIndex,
  });
  const msg: RatchetMessage = {
    kind: `ratchetKey`,
    data: {
      participantIdentity,
      keyIndex,
      ratchetResult,
    },
  };
  postMessage(msg);
}

function handleSifTrailer(trailer: Uint8Array) {
  sifTrailer = trailer;
  participantCryptors.forEach((c) => {
    c.setSifTrailer(trailer);
  });
}

// Operations using RTCRtpScriptTransform.
// @ts-ignore
if (self.RTCTransformEvent) {
  // TODO: HERE
  workerLogger.debug(`${E2EE_WORKER_LOG_PREFIX} setup transform event`, getLogContext());
  // @ts-ignore
  self.onrtctransform = (event: RTCTransformEvent) => {
    // @ts-ignore
    const transformer = event.transformer;
    workerLogger.debug(`${E2EE_WORKER_LOG_PREFIX} transformer info`, {
      ...getLogContext(),
      transformer,
    });

    const { kind, participantIdentity, trackId, codec } =
      transformer.options as ScriptTransformOptions;
    const cryptor = getTrackCryptor(participantIdentity, trackId);
    workerLogger.info(`${E2EE_WORKER_LOG_PREFIX} setting up transform. Codec info`, {
      ...getLogContext(),
      codec,
    });
    cryptor.setupTransform(kind, transformer.readable, transformer.writable, trackId, false, codec);
  };
}
