import { randomUUID } from 'node:crypto';
import { mkdir, rename, writeFile } from 'node:fs/promises';
import { basename, isAbsolute, resolve } from 'node:path';

const users = ['alice', 'bob', 'carol', 'dave'];
const outputArgument = process.argv[2];

if (!outputArgument) {
    console.error('Usage: npm run produce:events -- <folder>');
    process.exitCode = 1;
} else {
    const outputDirectory = isAbsolute(outputArgument)
        ? outputArgument
        : resolve(process.cwd(), outputArgument);
    void startProducer(outputDirectory);
}

async function startProducer(outputDirectory: string): Promise<void> {
    try {
        await mkdir(outputDirectory, { recursive: true });
        await writeEventFile(outputDirectory);
        const interval = setInterval(() => {
            void writeEventFile(outputDirectory).catch(error => {
                console.error('Could not write an event file:', error);
            });
        }, 1_000);
        process.once('SIGINT', () => clearInterval(interval));
    } catch (error) {
        console.error('Could not start the event producer:', error);
        process.exitCode = 1;
    }
}

async function writeEventFile(outputDirectory: string): Promise<void> {
    const event = {
        user: users[Math.floor(Math.random() * users.length)],
        amount: Math.round(Math.random() * 100_00) / 100,
        event_time: new Date().toISOString().slice(0, 19)
    };
    const id = randomUUID();
    const destination = resolve(outputDirectory, `events-${Date.now()}-${id}.json`);
    const temporary = resolve(outputDirectory, `.${basename(destination)}.tmp`);
    try {
        await writeFile(temporary, `${JSON.stringify(event)}\n`, { encoding: 'utf8', flag: 'wx' });
        await rename(temporary, destination);
        console.log(`Wrote ${destination}`);
    } catch (error) {
        throw new Error(`Could not atomically write event file in ${outputDirectory}.`, { cause: error });
    }
}
