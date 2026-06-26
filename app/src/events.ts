import { Response } from 'express';

// Log de ações que o cliente executa (conectar, add, pausar grupo...).
// Sem worker próprio, não há eventos de "processing/completed" — o movimento
// das colunas vem do polling de /api/jobs sobre os workers reais da aplicação-alvo.
export interface ClientEvent {
  timestamp: string;
  type: string;
  data: unknown;
}

export const eventStore: ClientEvent[] = [];
export const sseClients = new Set<Response>();

export function addEvent(type: string, data: unknown): void {
  const event: ClientEvent = { timestamp: new Date().toISOString(), type, data };
  eventStore.unshift(event);
  if (eventStore.length > 100) eventStore.pop();
  for (const client of sseClients) {
    try {
      client.write(`data: ${JSON.stringify(event)}\n\n`);
    } catch {
      sseClients.delete(client);
    }
  }
}
