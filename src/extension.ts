import * as vscode from 'vscode';
import { ClusterStore } from './flink/ClusterStore';
import { FlinkTreeProvider } from './tree/FlinkTreeProvider';

export function activate(context: vscode.ExtensionContext): void {
    const store = new ClusterStore(context.globalState);
    const provider = new FlinkTreeProvider(context, store);
    const tree = vscode.window.createTreeView('ApacheFlinkView', {
        treeDataProvider: provider,
        showCollapseAll: true
    });

    context.subscriptions.push(tree);
}
