export type SqlStatementKind = 'query' | 'insert' | 'ddl' | 'set' | 'show' | 'explain' | 'other';

export function splitSqlStatements(text: string): string[] {
    const statements: string[] = [];
    let current = '';
    let hasSql = false;
    let quote: 'single' | 'backtick' | undefined;
    let inLineComment = false;
    let inBlockComment = false;

    for (let index = 0; index < text.length; index++) {
        const character = text[index];
        const next = text[index + 1];

        if (inLineComment) {
            current += character;
            if (character === '\n' || character === '\r') {
                inLineComment = false;
            }
            continue;
        }
        if (inBlockComment) {
            current += character;
            if (character === '*' && next === '/') {
                current += next;
                index++;
                inBlockComment = false;
            }
            continue;
        }
        if (quote === 'single' || quote === 'backtick') {
            current += character;
            if (quote === 'single' && character === '\\' && next !== undefined) {
                current += next;
                index++;
            } else if (quote === 'single' && character === "'" && next === "'") {
                current += next;
                index++;
            } else if (quote === 'backtick' && character === '`' && next === '`') {
                current += next;
                index++;
            } else if ((quote === 'single' && character === "'") || (quote === 'backtick' && character === '`')) {
                quote = undefined;
            }
            continue;
        }

        if (character === '-' && next === '-') {
            current += `${character}${next}`;
            index++;
            inLineComment = true;
        } else if (character === '/' && next === '*') {
            current += `${character}${next}`;
            index++;
            inBlockComment = true;
        } else if (character === "'") {
            current += character;
            hasSql = true;
            quote = 'single';
        } else if (character === '`') {
            current += character;
            hasSql = true;
            quote = 'backtick';
        } else if (character === ';') {
            pushStatement();
        } else {
            current += character;
            if (!/\s/.test(character)) {
                hasSql = true;
            }
        }
    }

    pushStatement();
    return statements;

    function pushStatement(): void {
        if (hasSql) {
            const statement = current.trim();
            if (statement) {
                statements.push(statement);
            }
        }
        current = '';
        hasSql = false;
    }
}

export function classifyStatement(sql: string): SqlStatementKind {
    const initialKeyword = firstKeyword(sql);
    const keyword = initialKeyword === 'WITH' ? keywordAfterCte(sql) : initialKeyword;
    if (keyword === 'SELECT' || keyword === 'WITH' || keyword === 'VALUES' || keyword === 'TABLE') {
        return 'query';
    }
    if (keyword === 'INSERT') {
        return 'insert';
    }
    if (keyword === 'SET') {
        return 'set';
    }
    if (keyword === 'SHOW') {
        return 'show';
    }
    if (keyword === 'EXPLAIN') {
        return 'explain';
    }
    if (['CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'RENAME', 'COMMENT', 'GRANT', 'REVOKE'].includes(keyword)) {
        return 'ddl';
    }
    return 'other';
}

export function isReadOnlySqlAllowed(kind: SqlStatementKind): boolean {
    return kind === 'query' || kind === 'show' || kind === 'explain' || kind === 'set';
}

export function redactSqlLiterals(statement: string): string {
    let safe = '';
    for (let index = 0; index < statement.length; index++) {
        const character = statement[index];
        const next = statement[index + 1];
        if (character === "'") {
            safe += "'[literal]'";
            index++;
            while (index < statement.length) {
                if (statement[index] === '\\' && index + 1 < statement.length) {
                    index++;
                } else if (statement[index] === "'" && statement[index + 1] === "'") {
                    index += 2;
                } else if (statement[index] === "'") {
                    break;
                } else {
                    index++;
                }
            }
        } else if (character === '-' && next === '-') {
            safe += '--[comment]';
            index += 2;
            while (index < statement.length && statement[index] !== '\n' && statement[index] !== '\r') {
                index++;
            }
            if (index < statement.length) {
                safe += statement[index];
            }
        } else if (character === '/' && next === '*') {
            safe += '/*[comment]*/';
            index += 2;
            while (index < statement.length && !(statement[index] === '*' && statement[index + 1] === '/')) {
                index++;
            }
            index++;
        } else {
            safe += character;
        }
    }
    return safe.replace(
        /\b(password|passwd|secret|token|access[_-]?key|secret[_-]?key)\b\s*=\s*[^\s,;)]+/gi,
        '$1=[redacted]'
    );
}

function firstKeyword(sql: string): string {
    return firstKeywordMatch(sql)?.keyword ?? '';
}

function firstKeywordMatch(sql: string): { keyword: string; index: number } | undefined {
    let index = 0;
    while (index < sql.length) {
        if (/\s/.test(sql[index])) {
            index++;
        } else if (sql[index] === '-' && sql[index + 1] === '-') {
            index += 2;
            while (index < sql.length && sql[index] !== '\n' && sql[index] !== '\r') {
                index++;
            }
        } else if (sql[index] === '/' && sql[index + 1] === '*') {
            const end = sql.indexOf('*/', index + 2);
            if (end < 0) {
                return undefined;
            }
            index = end + 2;
        } else {
            break;
        }
    }

    const match = /^[A-Za-z]+/.exec(sql.slice(index));
    return match ? { keyword: match[0].toUpperCase(), index } : undefined;
}

function keywordAfterCte(sql: string): string {
    const first = firstKeywordMatch(sql);
    if (!first || first.keyword !== 'WITH') {
        return 'WITH';
    }
    let index = first.index + first.keyword.length;
    let depth = 0;
    let quote: 'single' | 'backtick' | undefined;
    let lineComment = false;
    let blockComment = false;

    while (index < sql.length) {
        const character = sql[index];
        const next = sql[index + 1];
        if (lineComment) {
            if (character === '\n' || character === '\r') {
                lineComment = false;
            }
            index++;
            continue;
        }
        if (blockComment) {
            if (character === '*' && next === '/') {
                blockComment = false;
                index += 2;
            } else {
                index++;
            }
            continue;
        }
        if (quote) {
            if (quote === 'single' && character === "'" && next === "'") {
                index += 2;
                continue;
            }
            if (quote === 'backtick' && character === '`' && next === '`') {
                index += 2;
                continue;
            }
            if ((quote === 'single' && character === "'") || (quote === 'backtick' && character === '`')) {
                quote = undefined;
            }
            index++;
            continue;
        }
        if (character === '-' && next === '-') {
            lineComment = true;
            index += 2;
            continue;
        }
        if (character === '/' && next === '*') {
            blockComment = true;
            index += 2;
            continue;
        }
        if (character === "'" || character === '`') {
            quote = character === "'" ? 'single' : 'backtick';
            index++;
            continue;
        }
        if (character === '(') {
            depth++;
            index++;
            continue;
        }
        if (character === ')') {
            depth = Math.max(0, depth - 1);
            index++;
            continue;
        }
        if (depth === 0 && /[A-Za-z_]/.test(character)) {
            const start = index;
            while (index < sql.length && /[A-Za-z0-9_]/.test(sql[index])) {
                index++;
            }
            const keyword = sql.slice(start, index).toUpperCase();
            if (['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'VALUES', 'TABLE', 'CREATE', 'DROP', 'ALTER'].includes(keyword)) {
                return keyword;
            }
            continue;
        }
        index++;
    }
    return 'WITH';
}
