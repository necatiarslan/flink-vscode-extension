export type PreviewRow = Record<string, unknown> & {
    kind?: string;
    rowKind?: string;
    $rowKind?: string;
};

export function isCompilerGeneratedPreviewSequence(statements: string[]): boolean {
    if (statements.length < 2) {
        return false;
    }
    const setup = statements.slice(0, -1);
    const hasSource = setup.some(statement => statement.startsWith('CREATE TEMPORARY TABLE `job_source` '));
    const allowedSetup = setup.every(statement =>
        /^SET '(?:pipeline\.name|parallelism\.default|execution\.checkpointing\.interval|table\.exec\.state\.ttl|table\.exec\.source\.idle-timeout)' = '(?:[^']|'')*'$/.test(statement)
        || statement.startsWith('CREATE TEMPORARY TABLE `job_source` ')
        || /^CREATE TEMPORARY VIEW `job_step_\d+` AS /.test(statement)
    );
    const select = statements[statements.length - 1];
    return hasSource
        && allowedSetup
        && /^SELECT (?!\*)[\s\S]+ FROM `job_(?:source|step_\d+)`$/.test(select)
        && !statements.some(statement => /\bINSERT\b|\bDROP\b|\bALTER\b/i.test(statement));
}

export function applyChangelog(
    rows: PreviewRow[],
    keyColumns: string[]
): PreviewRow[] | undefined {
    if (!keyColumns.length) {
        return undefined;
    }
    const state = new Map<string, PreviewRow>();
    for (const row of rows) {
        const hasExplicitKind = typeof row.$rowKind === 'string';
        const rowKind = hasExplicitKind ? row.$rowKind : row.kind ?? row.rowKind;
        const key = JSON.stringify(keyColumns.map(column => row[column]));
        if (rowKind === '-U' || rowKind === '-D') {
            state.delete(key);
            continue;
        }
        const value = { ...row };
        delete value.$rowKind;
        if (!hasExplicitKind) {
            delete value.kind;
            delete value.rowKind;
        }
        state.set(key, value);
    }
    return [...state.values()];
}
