import * as vscode from 'vscode';
import { ClusterStore } from './flink/ClusterStore';
import { JobDesignerController } from './jobdesigner/JobDesignerController';
import { SqlRunController } from './flink/SqlRunController';
import { FlinkTreeProvider } from './tree/FlinkTreeProvider';

export function activate(context: vscode.ExtensionContext): void {
    const store = new ClusterStore(context.globalState);
    const provider = new FlinkTreeProvider(context, store);
    new SqlRunController(context, store);
    new JobDesignerController(context, store, () => provider.refresh());
    const tree = vscode.window.createTreeView('ApacheFlinkView', {
        treeDataProvider: provider,
        showCollapseAll: true
    });

    context.subscriptions.push(tree);
}
