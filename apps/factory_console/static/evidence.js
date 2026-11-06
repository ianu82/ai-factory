import {
  escape as e,
  icon,
  badge,
  fullDate,
  relative,
  showDialog,
  api,
  notify,
} from "./core.js";

const fileURL = (file) => `/api/evidence/${file.id}`;
export function evidence(data, kind, target) {
  const files = (data.artifacts || []).filter(
    (f) =>
      (f.target_kind === kind && f.target_id === target.id) ||
      (target.run_id &&
        f.target_kind === "run" &&
        f.target_id === target.run_id),
  );
  const run =
    kind === "run" ? target : data.runs.find((r) => r.id === target.run_id);
  return `<section class="detail-section evidence-section"><div class="section-title">${icon("checkcircle")}<h2>Evidence</h2><button class="button secondary small" data-attach-evidence>${icon("plus")}Attach</button></div>
  ${run?.review ? `<details class="evidence-receipt" data-receipt="review"><summary>${icon("checkcircle")}Independent review ${badge(run.review.accepted ? "passed" : "unverified")}</summary><div class="receipt-body"><p>${run.review.criteria.map((c) => `${e(c.id)} · ${e(c.status)}`).join("<br>")}</p>${receiptFields({ commit_sha: run.review.candidate_sha, record_sha256: run.review.record_sha256 }, run.review.at)}</div></details>` : ""}
  ${(run?.checks || []).map((c, index) => `<details class="evidence-receipt" data-receipt="${e(c.record_sha256 || c.name + ":" + c.at + ":" + index)}"><summary><span>${e(c.name)}</span>${badge(c.status)}</summary><div class="receipt-body">${receiptFields(c, c.at)}</div></details>`).join("")}
  ${run?.environment ? `<details class="evidence-receipt" data-receipt="environment"><summary>${icon("layers")}Run environment</summary><dl class="receipt-body receipt-fields"><dt>Platform</dt><dd>${e(run.environment.platform || "Not recorded")}</dd><dt>Image</dt><dd class="mono">${e(run.environment.image || "Not recorded")}</dd><dt>Profile</dt><dd class="mono">${e(run.environment.profile_sha256 || "Not recorded")}</dd><dt>Runtime</dt><dd class="mono">${e(run.environment.runtime_sha || "Not recorded")}</dd><dt>VM identity</dt><dd>Not recorded in this journal</dd></dl></details>` : ""}
  ${files.length ? `<div class="evidence-files">${files.map((f) => `<button class="evidence-file" data-evidence-file="${e(f.id)}">${f.mime.startsWith("image/") ? `<img src="${fileURL(f)}" alt="${e(f.caption || f.name)}" loading="lazy">` : `<span class="evidence-file-icon">${icon("file")}</span>`}<span class="evidence-file-name">${e(f.caption || f.name)}</span><small>Manually attached · ${relative(f.at)}</small></button>`).join("")}</div>` : ""}
  ${!files.length && !run?.checks?.length && !run?.review ? `<p class="quiet-copy">No evidence attached</p>` : ""}</section>`;
}
function receiptFields(c, at) {
  return `<dl class="receipt-fields"><dt>Recorded</dt><dd>${e(fullDate(at))}</dd><dt>Candidate</dt><dd class="mono">${e(c.commit_sha || "Not recorded")}</dd>${c.exit_code !== undefined && c.exit_code !== null ? `<dt>Exit code</dt><dd>${c.exit_code}</dd>` : ""}${c.duration_seconds !== undefined && c.duration_seconds !== null ? `<dt>Duration</dt><dd>${c.duration_seconds}s</dd>` : ""}${c.sequence ? `<dt>Journal event</dt><dd>${c.sequence}</dd>` : ""}<dt>Record SHA256</dt><dd class="mono">${e(c.record_sha256 || "Not supplied by this observer")}</dd></dl>`;
}
export function bindEvidence(data, kind, target, refresh) {
  if (!target) return;
  document
    .querySelector("[data-attach-evidence]")
    ?.addEventListener("click", () => {
      showDialog(`<form id="evidence-form"><div class="dialog-heading"><h2>Attach evidence</h2><button type="button" class="icon-button" data-close aria-label="Close">${icon("close")}</button></div>
    <label>Screenshot or report<input id="evidence-file" type="file" accept="image/png,image/jpeg,.txt,.log,.md,.json" required></label><p class="field-note">PNG, JPEG or text · up to 1 MB. Attachments do not change verification status.</p>
    <label>Caption<input name="caption" maxlength="200" placeholder="e.g. Dashboard tested in the VM"></label><label>Candidate commit<input name="candidate_sha" class="mono" pattern="[0-9a-f]{40}" maxlength="40" value="${e(target.candidate_sha || "")}" placeholder="Optional full commit SHA"></label>
    <p id="evidence-error" role="alert"></p><div class="dialog-footer"><button type="button" class="button secondary" data-close>Cancel</button><button class="button primary">Attach evidence</button></div></form>`);
      let saving = false;
      document
        .querySelectorAll("#dialog [data-close]")
        .forEach(
          (b) => (b.onclick = () => document.querySelector("#dialog").close()),
        );
      document.querySelector("#evidence-form").onsubmit = async (event) => {
        event.preventDefault();
        if (saving) return;
        const form = event.target,
          file = form.querySelector("#evidence-file").files[0],
          button = form.querySelector(".primary");
        if (!file || file.size > 1024 * 1024) {
          form.querySelector("#evidence-error").textContent =
            "Choose a file up to 1 MB.";
          return;
        }
        saving = true;
        button.disabled = true;
        try {
          const content = await new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(reader.result.split(",")[1]);
            reader.onerror = reject;
            reader.readAsDataURL(file);
          });
          await api("/api/evidence", {
            ...Object.fromEntries(new FormData(form)),
            target_kind: kind,
            target_id: target.id,
            name: file.name,
            content_base64: content,
          });
          document.querySelector("#dialog").close();
          await refresh();
          notify("Evidence attached");
        } catch (error) {
          form.querySelector("#evidence-error").textContent =
            error.message || "The file could not be attached.";
        } finally {
          saving = false;
          button.disabled = false;
        }
      };
    });
  document.querySelectorAll("[data-evidence-file]").forEach(
    (button) =>
      (button.onclick = () => {
        const file = data.artifacts.find(
          (f) => f.id === button.dataset.evidenceFile,
        );
        showDialog(
          `<div class="dialog-heading"><h2>${e(file.caption || file.name)}</h2><button class="icon-button" data-close aria-label="Close">${icon("close")}</button></div>${file.mime.startsWith("image/") ? `<img class="evidence-preview" src="${fileURL(file)}" alt="${e(file.caption || file.name)}">` : ""}<p class="field-note">Manually attached · ${e(fullDate(file.at))}</p><dl class="receipt-fields"><dt>File</dt><dd>${e(file.name)}</dd><dt>Candidate</dt><dd class="mono">${e(file.candidate_sha || "Not specified")}</dd><dt>SHA256</dt><dd class="mono">${e(file.sha256)}</dd></dl><div class="dialog-footer"><a class="button secondary" href="${fileURL(file)}" target="_blank" rel="noopener noreferrer">Open original ${icon("external")}</a></div>`,
        );
      }),
  );
}
