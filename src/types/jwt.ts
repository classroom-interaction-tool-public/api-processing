export interface JWTPayload {
  userIdentifier: string;
  isAnonymous?: boolean;
  sessionCode?: string;
  sessionId?: string;
}
