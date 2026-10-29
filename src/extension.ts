import * as vscode from 'vscode';
import * as ui from './common/UI';
import { Session } from './common/Session';
import { TreeView } from './tree/TreeView';
import { ConnectionView } from './tree/ConnectionView';
import { ServiceHub } from './tree/ServiceHub';
import { TreeState } from './tree/TreeState';


/**
 * Activates the Apache Flink extension.
 * This is the entry point for the extension.
 */
export function activate(context: vscode.ExtensionContext): void {
    ui.logToOutput('Activating Apache Flink...');

    try {
        const session = new Session(context); // Initialize session management
        new ServiceHub(context);    // Initialize service hub
        
        // 1. Initialize the Unified "Apache Flink" Tree Provider
        new TreeView(context);

        // 1.1 Initialize Aws Connection Tree Provider
        new ConnectionView(context);

        // 2. Load saved tree state after TreeView is initialized
        TreeState.load();
        
        // 3. Refresh tree to display loaded nodes
        TreeView.Current.Refresh();


        ui.logToOutput('Apache Flink activated successfully.');
    } catch (error) {
        ui.logToOutput('Fatal error activating Apache Flink:', error as Error);
        ui.showInfoMessage('Apache Flink failed to activate. Check debug console for details.');
    }
}



export function deactivate(): void {
    // Save tree state immediately before deactivation
    TreeState.saveImmediate();
}
