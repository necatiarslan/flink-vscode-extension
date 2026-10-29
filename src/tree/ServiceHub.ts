import * as vscode from 'vscode';
import { FileSystemService } from "../filesystem/FileSystemService";
import { VscodeService } from '../vscode/VscodeService';


export class ServiceHub {
    public static Current: ServiceHub;
    public Context: vscode.ExtensionContext;
    public FileSystemService: FileSystemService = new FileSystemService();
    public VscodeService: VscodeService = new VscodeService();
    
    public constructor(context: vscode.ExtensionContext) {
        this.Context = context;
        ServiceHub.Current = this;
    }

}