// routes/v1/session.routes.ts
import { FastifyInstance, FastifyPluginOptions, FastifyReply, FastifyRequest } from 'fastify';

import * as ampq from 'amqplib/callback_api';
import { getEnvVariable } from '../common/src/utils/config';
import AnswerModel from '../common/src/mongoose-schemas/v1/answer.schema';
var amqp = require('amqplib/callback_api');

const RABBITMQ_USERNAME = getEnvVariable('RABBITMQ_USERNAME') || 'rabbitmq_user';
const RABBITMQ_PASSWORD = getEnvVariable('RABBITMQ_PASSWORD') || 'rabbitmq_password';
const RABBITMQ_HOSTNAME = getEnvVariable('RABBITMQ_HOSTNAME') || 'rabbitmq_host';
const RABBITMQ_PORT = getEnvVariable('RABBITMQ_PORT') || 'rabbitmq_port';
const RABBITMQ_QUEUE = getEnvVariable('RABBITMQ_QUEUE') || 'rabbitmq_queue';
const RABBITMQ_QUEUE_NAME = getEnvVariable('RABBITMQ_QUEUE_NAME') || 'rabbitmq_queue_name';

const RABBITMQ_URI = `amqp://${RABBITMQ_USERNAME}:${RABBITMQ_PASSWORD}@${RABBITMQ_HOSTNAME}:${RABBITMQ_PORT}/${RABBITMQ_QUEUE}`;

export default function sessionRoutes(fastify: FastifyInstance, options: FastifyPluginOptions, done: () => void) {
  options = {
    ...options,
    onRequest: [fastify.authenticate],
    schema: {
      querystring: {
        type: 'object',
        properties: {
          isActive: { type: 'string' },
        },
      },
    },
  };

  let channel: ampq.Channel | null = null;
  const sessionClients: Map<string, Set<FastifyReply>> = new Map();

  console.log(`Connecting to RabbitMQ at ${RABBITMQ_URI}`);

  const connectToRabbitMQ = () => {
    amqp.connect(
      RABBITMQ_URI,
      function (error0: any, connection: ampq.Connection) {
        if (error0) {
          throw error0;
        }

        connection.createChannel(function (error1: any, ch: ampq.Channel) {
          if (error1) {
            throw error1;
          }

          console.log(`Joined channel ${RABBITMQ_QUEUE_NAME}`);
          channel = ch;
          channel.assertQueue(RABBITMQ_QUEUE_NAME, { durable: false });

          channel.consume(
            RABBITMQ_QUEUE_NAME,
            function (msg) {
              if (msg) {
                const message = JSON.parse(msg.content.toString());
                const sessionId = message.sessionId;
                broadcastEvent(sessionId, message);
              }
            },
            { noAck: true }
          );
        });
      }
    );
  };

  const broadcastEvent = (sessionId: string, data: any) => {
    const sseFormattedResponse = `${JSON.stringify(data)}\n\n`;
    const clients = sessionClients.get(sessionId) || new Set();

    clients.forEach(client => {
      client.raw.write(sseFormattedResponse);
    });
  };

  fastify.get(
    '/session/:sessionId/question/:questionId/answers/events',
    async (request: FastifyRequest, reply: FastifyReply) => {
      reply.raw.setHeader('Access-Control-Allow-Origin', '*');
      reply.raw.setHeader('Content-Type', 'text/event-stream');
      reply.raw.setHeader('Cache-Control', 'no-cache');
      reply.raw.setHeader('Connection', 'keep-alive');

      const { sessionId, questionId } = request.params as { sessionId: string; questionId: string };

      if (!channel) {
        connectToRabbitMQ();
      }

      if (!sessionClients.has(sessionId)) {
        sessionClients.set(sessionId, new Set());
      }

      const clients = sessionClients.get(sessionId);
      clients?.add(reply);
      const responses = await AnswerModel.find({ sessionId, questionId });

      if (responses.length > 0) {
        responses.forEach((response: any) => {
          if (response.content.type && response.content.value) {
            broadcastEvent(sessionId, {
              content: { type: response.content.type, value: response.content.value },
              aid: response._id,
            });
          }
        });
      } else {
        broadcastEvent(sessionId, { type: 'freeText', value: '' });
      }

      request.raw.on('close', () => {
        const clients = sessionClients.get(sessionId);
        if (clients) {
          clients.delete(reply);
          if (clients.size === 0) {
            sessionClients.delete(sessionId);
          }
        }
      });
    }
  );

  done();
}
