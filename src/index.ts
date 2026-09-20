export type EventRecord={sequence:number;stream:string;payload:unknown};
export class EventLog{#events:EventRecord[]=[];append(stream:string,payload:unknown){const event={sequence:this.#events.length+1,stream,payload};this.#events.push(event);return event}read(from=1){return this.#events.filter(event=>event.sequence>=from)}watermark(){return this.#events.length}}
