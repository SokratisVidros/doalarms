export interface Env {
	BATCHER: DurableObjectNamespace;
}

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		let id = env.BATCHER.idFromName('foo');
		return await env.BATCHER.get(id).fetch(request);
	},
};

const SECONDS = 1000;

export class Batcher {
	state: DurableObjectState;
	storage: DurableObjectStorage;
	count: number = 0;

	constructor(state: DurableObjectState, env: Env) {
		this.state = state;
		this.storage = state.storage;
		this.state.blockConcurrencyWhile(async () => {
			let vals = await this.storage.list({ reverse: true, limit: 1 });
			this.count = vals.size == 0 ? 0 : parseInt(vals.keys().next().value);
		});
		console.log('Batcher:> Initializing DO');
	}

	async fetch(request: Request) {
		this.count++;

		// If there is no alarm currently set, set one for 10 seconds from now
		// Any further POSTs in the next 10 seconds will be part of this batch.
		let currentAlarm = await this.storage.getAlarm();
		if (currentAlarm == null) {
			this.storage.setAlarm(Date.now() + 2 * SECONDS);
		}

		console.log('Batcher:> Current alarm %s', await this.storage.getAlarm());

		// Add the request to the batch.
		await this.storage.put(this.count.toString(), await request.text());
		return new Response(JSON.stringify({ queued: this.count }), {
			headers: {
				'content-type': 'application/json;charset=UTF-8',
			},
		});
	}

	async alarm() {
		let vals = await this.storage.list();
		console.log('Batcher:> Values %s', Array.from(vals.values()));
		await this.storage.deleteAll();
		this.count = 0;
	}
}
