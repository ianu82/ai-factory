/** Keep following streamed output only while the reader is near the bottom.
 * Remember the pre-update position: measuring after a token has grown the pane
 * would incorrectly interpret new content as the person scrolling away.
 */
export interface MindMessagePane {
  scrollTop: number;
  scrollHeight: number;
  clientHeight: number;
}
export class MindScroll {
  private following = true;
  private unseen = false;
  onScroll(pane: MindMessagePane): boolean {
    this.following = pane.scrollHeight - pane.clientHeight - pane.scrollTop <= 40;
    if (this.following) this.unseen = false;
    return this.unseen;
  }
  onOutput(pane: MindMessagePane): boolean {
    if (this.following) this.jump(pane);
    else this.unseen = true;
    return this.unseen;
  }
  jump(pane: MindMessagePane): void {
    pane.scrollTop = Math.max(0, pane.scrollHeight - pane.clientHeight);
    this.following = true;
    this.unseen = false;
  }
}
