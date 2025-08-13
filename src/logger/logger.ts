export class Logger {
    private static _instance: Logger | null = null;

    constructor() { }

    // Static method to get the singleton instance
    public static getInstance(): Logger {
        if (!Logger._instance) {
            Logger._instance = new Logger();
        }
        return Logger._instance;
    }

    info(...args: any[]): void {
        console.log(`[INFO]`, new Date().toISOString(), ...args);
    }

    warn(...args: any[]): void {
        console.warn(`[WARN]`, new Date().toISOString(), ...args);
    }

    error(...args: any[]): void {
        console.error(`[ERROR]`, new Date().toISOString(), ...args);
    }

    debug(...args: any[]): void {
        if (process.env.DEBUG) {
            console.debug(`[DEBUG]`, new Date().toISOString(), ...args);
        }
    }
}
