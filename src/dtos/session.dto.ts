// src/dtos/session.dto.ts

export interface SessionDTO {
  id: string;
  sessionCode: string;
  ownerId: string;
  allowAnonymous: boolean;
  questionCollectionIds?: string[];
}
