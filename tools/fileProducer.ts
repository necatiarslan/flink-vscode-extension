import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

const words = ['flink', 'typescript', 'stream', 'docker'];
const inputDirectory = join(process.cwd(), 'flink-data', 'input');

async function writeWordFile(): Promise<void> {
    await mkdir(inputDirectory, { recursive: true });
    const line = Array.from({ length: 5 }, () => words[Math.floor(Math.random() * words.length)]).join(' ');
    const filePath = join(inputDirectory, `words-${Date.now()}-${randomUUID()}.txt`);
    await writeFile(filePath, `${line}\n`, { encoding: 'utf8', flag: 'wx' });
    console.log(`Wrote ${filePath}`);
}

void writeWordFile()
    .then(() => {
        const interval = setInterval(() => {
            void writeWordFile().catch(error => {
                console.error('Could not write a word file:', error);
            });
        }, 1_000);
        process.once('SIGINT', () => {
            clearInterval(interval);
        });
    })
    .catch(error => {
        console.error('Could not start the file producer:', error);
        process.exitCode = 1;
    });
