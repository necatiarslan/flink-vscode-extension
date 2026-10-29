"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ServiceHub = void 0;
const FileSystemService_1 = require("../filesystem/FileSystemService");
const VscodeService_1 = require("../vscode/VscodeService");
class ServiceHub {
    static Current;
    Context;
    FileSystemService = new FileSystemService_1.FileSystemService();
    VscodeService = new VscodeService_1.VscodeService();
    constructor(context) {
        this.Context = context;
        ServiceHub.Current = this;
    }
}
exports.ServiceHub = ServiceHub;
//# sourceMappingURL=ServiceHub.js.map