import { api, type Meta, type ReviewPolicy } from "./api";

export type ReviewPatch = { [K in keyof ReviewPolicy]?: ReviewPolicy[K] | null };

/** Unsupported servers must keep saved policies untouched, not write hidden defaults. */
export async function saveReviewPolicy(
  meta: Pick<Meta, "capabilities">, admin: boolean, workspaceId: string, review: ReviewPatch,
  repoId?: string, request: typeof api = api,
): Promise<void> {
  if (meta.capabilities?.review_gating !== true || !admin) return;
  const path = `/api/workspaces/${workspaceId}`;
  await request(repoId ? `${path}/repositories/${repoId}` : path, {
    method: "PATCH",
    json: repoId ? { config: {
      review: Object.fromEntries(Object.entries(review).filter(([, value]) => value !== null)),
      review_inherit: Object.keys(review).filter((key) => review[key as keyof ReviewPolicy] === null),
    } } : { settings: { review } },
  });
}
