import * as vscode from "vscode";
import { NodeBase } from "./NodeBase";
import { TreeProvider } from "./TreeProvider";
import { Session } from "../common/Session";
import { ServiceHub } from "./ServiceHub";
import { TreeState } from "./TreeState";
import * as ui from "../common/UI";

export class TreeView {

    private static readonly FREE_NODE_LIMIT = 10;

    public static Current: TreeView;
	public view: vscode.TreeView<NodeBase>;
	public treeDataProvider: TreeProvider;
	public context: vscode.ExtensionContext;

    constructor(context: vscode.ExtensionContext) {
		TreeView.Current = this;
		this.context = context;
		this.treeDataProvider = new TreeProvider();
		this.view = vscode.window.createTreeView('ApacheFlinkView', { treeDataProvider: this.treeDataProvider, showCollapseAll: true });
		context.subscriptions.push(this.view);
        this.RegisterCommands();
        this.view.onDidChangeSelection(e => { if (e.selection.length > 0) { this.NodeSelect(e.selection[0]); } }, null, context.subscriptions);
        this.view.onDidExpandElement(e => { this.NodeExpand(e.element); }, null, context.subscriptions);
        this.view.onDidCollapseElement(e => { this.NodeCollapse(e.element); }, null, context.subscriptions);
	}

    public RegisterCommands(): void {
        vscode.commands.registerCommand('ApacheFlink.Refresh', () => {
            this.Refresh();
        });

        vscode.commands.registerCommand('ApacheFlink.Filter', () => {
            this.Filter();
        });

        vscode.commands.registerCommand('ApacheFlink.ShowOnlyFavorite', () => {
            this.ShowOnlyFavorite();
        });

        vscode.commands.registerCommand('ApacheFlink.ShowHidden', () => {
            this.ShowHidden();
        });

        vscode.commands.registerCommand('ApacheFlink.SelectAwsProfile', () => {
            this.SelectAwsProfile();
        });

        vscode.commands.registerCommand('ApacheFlink.TestAwsConnection', () => {
            this.TestAwsConnection();
        });

        vscode.commands.registerCommand('ApacheFlink.RefreshCredentials', () => {
            this.RefreshCredentials();
        });

        vscode.commands.registerCommand('ApacheFlink.SetAwsRegion', () => {
            this.SetAwsRegion();
        });

        vscode.commands.registerCommand('ApacheFlink.UpdateAwsEndPoint', () => {
            this.UpdateAwsEndPoint();
        });

        vscode.commands.registerCommand('ApacheFlink.Hide', (node: NodeBase) => {
            this.Hide(node);
        });

        vscode.commands.registerCommand('ApacheFlink.UnHide', (node: NodeBase) => {
            this.UnHide(node);
        });

        vscode.commands.registerCommand('ApacheFlink.AddFav', (node: NodeBase) => {
            this.AddFav(node);
        });

        vscode.commands.registerCommand('ApacheFlink.RemoveFav', (node: NodeBase) => {
            this.RemoveFav(node);
        });

        vscode.commands.registerCommand('ApacheFlink.ShowOnlyInThisProfile', (node: NodeBase) => {
            this.ShowOnlyInThisProfile(node);
        });

        vscode.commands.registerCommand('ApacheFlink.ShowInAnyProfile', (node: NodeBase) => {
            this.ShowInAnyProfile(node);
        });

        vscode.commands.registerCommand('ApacheFlink.ShowOnlyInThisWorkspace', (node: NodeBase) => {
            this.ShowOnlyInThisWorkspace(node);
        });

        vscode.commands.registerCommand('ApacheFlink.ShowInAnyWorkspace', (node: NodeBase) => {
            this.ShowInAnyWorkspace(node);
        });

        vscode.commands.registerCommand('ApacheFlink.NodeAdd', (node: NodeBase) => {
            this.NodeAdd(node);
        });

        vscode.commands.registerCommand('ApacheFlink.NodeRemove', (node: NodeBase) => {
            this.NodeRemove(node);
        });

        vscode.commands.registerCommand('ApacheFlink.NodeRefresh', (node: NodeBase) => {
            this.NodeRefresh(node);
        });

        vscode.commands.registerCommand('ApacheFlink.NodeView', (node: NodeBase) => {
            this.NodeView(node);
        });

        vscode.commands.registerCommand('ApacheFlink.NodeEdit', (node: NodeBase) => {
            this.NodeEdit(node);
        });

        vscode.commands.registerCommand('ApacheFlink.NodeRun', (node: NodeBase) => {
            this.NodeRun(node);
        });

        vscode.commands.registerCommand('ApacheFlink.NodeStop', (node: NodeBase) => {
            this.NodeStop(node);
        });

        vscode.commands.registerCommand('ApacheFlink.NodeOpen', (node: NodeBase) => {
            this.NodeOpen(node);
        });

        vscode.commands.registerCommand('ApacheFlink.NodeInfo', (node: NodeBase) => {
            this.NodeInfo(node);
        });

        vscode.commands.registerCommand('ApacheFlink.NodeCopy', (node: NodeBase) => {
            this.NodeCopy(node);
        });

        vscode.commands.registerCommand('ApacheFlink.NodeAlias', (node: NodeBase) => {
            this.NodeAlias(node);
        });

        vscode.commands.registerCommand('ApacheFlink.SetTooltip', (node: NodeBase) => {
            this.SetTooltip(node);
        });

        vscode.commands.registerCommand('ApacheFlink.SetColor', (node: NodeBase) => {
            this.SetColor(node);
        });

        vscode.commands.registerCommand('ApacheFlink.MoveUp', (node: NodeBase) => {
            this.MoveUp(node);
        });

        vscode.commands.registerCommand('ApacheFlink.MoveDown', (node: NodeBase) => {
            this.MoveDown(node);
        });

        vscode.commands.registerCommand('ApacheFlink.MoveToFolder', (node: NodeBase) => {
            this.MoveToFolder(node);
        });

        vscode.commands.registerCommand('ApacheFlink.BugAndNewFeatureRequest', () => {
            this.BugAndNewFeatureRequest();
        });

        vscode.commands.registerCommand('ApacheFlink.Donate', () => {
            this.Donate();
        });

        vscode.commands.registerCommand('ApacheFlink.ExportConfig', () => {
            this.ExportConfig();
        });

        vscode.commands.registerCommand('ApacheFlink.ImportConfig', () => {
            this.ImportConfig();
        }); 

        vscode.commands.registerCommand('ApacheFlink.Add', (node: NodeBase) => {
            this.Add(undefined);
        });

        vscode.commands.registerCommand('ApacheFlink.Remove', (node: NodeBase) => {
            this.Remove(node);
        });
    }

	GetBoolenSign(value: boolean): string {
		return value ? "✓ " : "✗ ";
	}

	public async SetViewMessage() {
        const visibleNodeCount = NodeBase.RootNodes.filter(node => node.IsVisible).length;
		if (visibleNodeCount > 0) {
			this.view.message = 
				await this.GetFilterProfilePrompt()
				+ this.GetBoolenSign(Session.Current.IsShowOnlyFavorite) + "Fav, " 
				+ this.GetBoolenSign(Session.Current.IsShowHiddenNodes) + "Hidden, "
				+ (Session.Current.FilterString ? `Filter: ${Session.Current.FilterString}` : "");
		}
	}

	async GetFilterProfilePrompt() {
		return "Profile:" + Session.Current.AwsProfile + " ";
	}

    private IsUnrestrictedFreeNodeType(nodeType: string): boolean {
        return nodeType === "Folder" || nodeType === "Note";
    }

    private CountRestrictedNodes(nodes: NodeBase[]): number {
        let count = 0;

        for (const currentNode of nodes) {
            const nodeType = currentNode.constructor.name;
            if (nodeType !== "FolderNode" && nodeType !== "NoteNode") {
                count++;
            }

            if (currentNode.Children.length > 0) {
                count += this.CountRestrictedNodes(currentNode.Children);
            }
        }

        return count;
    }

    private CanAddNodeType(nodeType: string): boolean {
        if (Session.Current.IsProVersion || this.IsUnrestrictedFreeNodeType(nodeType)) {
            return true;
        }

        const restrictedNodeCount = this.CountRestrictedNodes(NodeBase.RootNodes);
        if (restrictedNodeCount >= TreeView.FREE_NODE_LIMIT) {
            ui.showWarningMessage("Free version can add up to 5 nodes. Upgrade to Pro version to add unlimited nodes.");
            // call ApacheFlink.ProUpgrade() to show upgrade prompt
            vscode.commands.executeCommand('ApacheFlink.ActivatePro');
            return false;
        }

        return true;
    }

    public async Remove(node: NodeBase): Promise<void> {
        if(!node){ return; }

        node.Remove();
        TreeState.save();
    }
    public async Add(node?: NodeBase): Promise<void> {
        
        const result:string[] = [];
        result.push("Folder");
        result.push("Note");
        result.push("File Link");
        result.push("Bash Script");
        result.push("Bash File");
        result.push("S3 Bucket");
        result.push("CloudWatch Log Group");
        result.push("Lambda Function");
        result.push("Step Function");
        result.push("Glue Job");
        result.push("DynamoDB Table");
        result.push("Sns Topic");
        result.push("Sqs Queue");
        result.push("IAM Role");
        result.push("IAM Policy");
        result.push("EMR Cluster");
        result.push("Vscode Command");
        let nodeType = await vscode.window.showQuickPick(result, {canPickMany:false, placeHolder: 'Select Item Type'});

        if(!nodeType){ return; }
        if(!this.CanAddNodeType(nodeType)){ return; }

        switch (nodeType) {
            case "Folder":
                await ServiceHub.Current.FileSystemService.Add(node, "Folder");
                break;
            case "Note":
                await ServiceHub.Current.FileSystemService.Add(node, "Note");
                break;
            case "File Link":
                await ServiceHub.Current.FileSystemService.Add(node, "File Link");
                break;
            case "Bash Script":
                await ServiceHub.Current.FileSystemService.Add(node, "Bash Script");
                break;
            case "Bash File":
                await ServiceHub.Current.FileSystemService.Add(node, "Bash File");
                break;
             case "Vscode Command":
                await ServiceHub.Current.VscodeService.Add(node, "Command");
                break;
         }
        TreeState.save();
    }

    public Refresh(node?: NodeBase): void {
        this.treeDataProvider.Refresh(node);
        this.SetViewMessage();
    }

    public async Filter(): Promise<void> {
        const filterString = await vscode.window.showInputBox({ placeHolder: 'Enter filter string', value: Session.Current.FilterString });
        if(filterString === undefined){ return; }
        
        Session.Current.FilterString = filterString;
        Session.Current.SaveState();
        NodeBase.RootNodes.forEach(node => {
            node.SetVisible();
        });
        this.Refresh();
    }

    public ShowOnlyFavorite(): void {
        Session.Current.IsShowOnlyFavorite = !Session.Current.IsShowOnlyFavorite;
        Session.Current.SaveState();
        NodeBase.RootNodes.forEach(node => {
            node.SetVisible();
        });
        this.Refresh();
    }

    public ShowHidden(): void {
        Session.Current.IsShowHiddenNodes = !Session.Current.IsShowHiddenNodes;
        Session.Current.SaveState();
        NodeBase.RootNodes.forEach(node => {
            node.SetVisible();
        });
        this.Refresh();
    }

    public async SelectAwsProfile(): Promise<void> {
        await Session.Current.SetAwsProfile();
        NodeBase.RootNodes.forEach(node => {
            node.SetVisible();
        });
        this.Refresh();
    }

    public TestAwsConnection(): void {
        Session.Current.TestAwsConnection();
    }

    public RefreshCredentials(): void {
        Session.Current.RefreshCredentials();
        ui.showInfoMessage("AWS credentials refreshed.");
    }

    public SetAwsRegion(): void {
        Session.Current.SetAwsRegion();
    }

    public UpdateAwsEndPoint(): void {
        Session.Current.SetAwsEndpoint();
    }

    public Hide(node: NodeBase): void {
        node.IsHidden = true;
        node.SetVisible();
        this.Refresh(node);
        TreeState.save();
    }

    public UnHide(node: NodeBase): void {
        node.IsHidden = false;
        node.SetVisible();
        this.Refresh(node);
        TreeState.save();
    }

    public AddFav(node: NodeBase): void {
        node.IsFavorite = true;
        node.SetVisible();
        this.Refresh(node);
        TreeState.save();
    }

    public RemoveFav(node: NodeBase): void {
        node.IsFavorite = false;
        node.SetVisible();
        this.Refresh(node);
        TreeState.save();
    }

    public ShowOnlyInThisProfile(node: NodeBase): void {
        node.AwsProfile = Session.Current.AwsProfile;
        node.SetContextValue();
        this.treeDataProvider.Refresh(node);
        TreeState.save();
    }

    public ShowInAnyProfile(node: NodeBase): void {
        node.AwsProfile = "";
        node.SetContextValue();
        this.treeDataProvider.Refresh(node);
        TreeState.save();
    }

    public ShowOnlyInThisWorkspace(node: NodeBase): void {
        if(!vscode.workspace.name){ 
            ui.showInfoMessage("Please open a workspace first.");
            return; 
        }

        node.Workspace =  vscode.workspace.name;
        node.SetContextValue();
        this.treeDataProvider.Refresh(node);
        TreeState.save();
    }

    public ShowInAnyWorkspace(node: NodeBase): void {
        node.Workspace = "";
        node.SetContextValue();
        this.treeDataProvider.Refresh(node);
        TreeState.save();
    }

    public NodeAdd(node: NodeBase): void {
        node.NodeAdd();
    }

    public NodeSelect(node: NodeBase): void {
        node.NodeSelect();
    }

    public NodeExpand(node: NodeBase): void {
        node.NodeExpand();
    }

    public NodeCollapse(node: NodeBase): void {
        node.NodeCollapse();
    }

    public NodeRemove(node: NodeBase): void {
        node.NodeRemove();
    }

    public NodeRefresh(node: NodeBase): void {
        node.NodeRefresh();
    }

    public NodeView(node: NodeBase): void {
        node.NodeView();
    }

    public NodeEdit(node: NodeBase): void {
        node.NodeEdit();
    }

    public NodeRun(node: NodeBase): void {
        node.NodeRun();
    }

    public NodeStop(node: NodeBase): void {
        node.NodeStop();
    }

    public NodeOpen(node: NodeBase): void {
        node.NodeOpen();
    }

    public NodeInfo(node: NodeBase): void {
        node.NodeInfo();
    }

    public NodeCopy(node: NodeBase): void {
        node.NodeCopy();
    }

    public NodeAlias(node: NodeBase): void {
        node.NodeAlias();
    }

    public SetTooltip(node: NodeBase): void {
        node.SetCustomTooltip();
    }

    public SetColor(node: NodeBase): void {
        node.SetIconColor();
    }

    public MoveUp(node: NodeBase): void {
        node.MoveUp();
    }

    public MoveDown(node: NodeBase): void {
        node.MoveDown();
    }

    public async MoveToFolder(node: NodeBase): Promise<void> {
        // Collect all FolderNode instances recursively
        const folders: { label: string; node: NodeBase }[] = [];
        
        const collectFolders = (nodes: NodeBase[], path: string = "") => {
            for (const n of nodes) {
                if (n.constructor.name === 'FolderNode') {
                    const fullPath = path ? `${path}/${n.label}` : n.label as string;
                    folders.push({ label: fullPath, node: n });
                }
                if (n.Children.length > 0) {
                    const newPath = n.constructor.name === 'FolderNode' 
                        ? (path ? `${path}/${n.label}` : n.label as string) 
                        : path;
                    collectFolders(n.Children, newPath);
                }
            }
        };
        
        collectFolders(NodeBase.RootNodes);
        
        if (folders.length === 0) {
            vscode.window.showInformationMessage('No folders available. Create a folder first.');
            return;
        }
        
        const selected = await vscode.window.showQuickPick(
            folders.map(f => f.label),
            { placeHolder: 'Select destination folder' }
        );
        
        if (!selected) { return; }
        
        const targetFolder = folders.find(f => f.label === selected)?.node;
        if (targetFolder && targetFolder !== node && targetFolder !== node.Parent) {
            node.MoveToFolder(targetFolder);
        }
    }

	public BugAndNewFeatureRequest(): void {
		vscode.env.openExternal(vscode.Uri.parse('https://github.com/necatiarslan/aws-workbench/issues/new'));
	}
	public Donate(): void {
		vscode.env.openExternal(vscode.Uri.parse('https://github.com/sponsors/necatiarslan'));
	}

    public async ExportConfig(): Promise<void> {
        const filePath = await vscode.window.showSaveDialog({
            defaultUri: vscode.Uri.file('aws-workbench.json'),
            saveLabel: 'Save',
            filters: {'JSON': ['json']},
        });
        if (!filePath) { return; }
        TreeState.save(filePath.fsPath);
    }

    public async ImportConfig(): Promise<void> {
        const filePath = await vscode.window.showOpenDialog({
            canSelectMany: false,
            canSelectFiles: true,
            canSelectFolders: false,
            defaultUri: vscode.Uri.file('aws-workbench.json'),
            filters: {'JSON': ['json']},
        });
        if (!filePath) { return; }
        TreeState.load(filePath[0].fsPath);
        TreeState.saveImmediate();
        this.Refresh();
    }

}