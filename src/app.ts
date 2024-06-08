// src/app.ts
import 'dotenv/config';

import { FastifyReply, FastifyRequest } from 'fastify';

import fjwt from '@fastify/jwt';
import cors from '@fastify/cors';

import sessionRoutes from './routes/session.routes';
import { getEnvVariable } from './common/src/utils/config';
import logger from './common/src/utils/logger';
import dbConnectorPlugin from './common/src/plugins/db.plugin';

interface JWTPayload {
  userIdentifier: string;
  sessionId: string;
  isAnonymous: boolean;
  sessionCode: string;
}

const app = require('fastify')({
  logger: {
    level: 'info',
    transport: {
      target: 'pino-pretty',
      options: {
        translateTime: 'HH:MM:ss Z',
        ignore: 'pid,hostname',
      },
    },
  },
});

app.register(cors, {
  origin: ['*'],
  methods: ['GET', 'PUT', 'POST', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: false,
  preflightContinue: true,
  optionsSuccessStatus: 204,
});

app.register(fjwt, {
  secret: getEnvVariable('JWT_SECRET'),
});

app.decorate(
  'authenticate',
  async function (request: FastifyRequest<{ Params: { sid: string } }>, reply: FastifyReply) {
    try {
      logger.info('Authenticating...');
      await request.jwtVerify();
      logger.info('Authenticated');

      const payload = request.user as JWTPayload;
      if (payload.isAnonymous && request.params.sid !== payload.sessionId) {
        logger.info('Anonymous user trying to access another session with same token');
        reply.status(401).send({ error: 'Unauthorized' });
      }
    } catch (err) {
      logger.error('Error authenticating:', err);
      reply.status(401).send({ error: 'Unauthorized' });
    }
  }
);

app.register(dbConnectorPlugin);

app.register(sessionRoutes);

export default app;
