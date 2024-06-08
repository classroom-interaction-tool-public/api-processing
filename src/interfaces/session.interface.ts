// src/interfaces/v1/session.interface.ts

export interface Session {
  id: string;
  sessionCode: string;
  ownerId: string;
  allowAnonymous: boolean;
  questionCollectionIds?: string[];
}
