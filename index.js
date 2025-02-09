const fs = require('fs');
const WebSocket = require('ws');
const SocksProxyAgent = require('socks-proxy-agent');
const { HttpsProxyAgent } = require('https-proxy-agent');
const banner = require('./banner');
const chalk = require('chalk');

class WebSocketBot {
    constructor() {
        this.config = this.loadConfig();
        this.proxies = this.loadProxies();
        this.connections = new Map();
        this.proxyIndex = 0;
        this.pingInterval = 25000;
        this.pingTimeout = 20000;
        this.deviceVersion = '0.7.0';
    }

    loadConfig() {
        try {
            const data = fs.readFileSync('config.json', 'utf8');
            const config = JSON.parse(data);
            
            if (!config.deviceId || !config.tokens || !Array.isArray(config.tokens)) {
                throw new Error('Invalid config format. Required fields: deviceId, tokens (array)');
            }
            
            return config;
        } catch (error) {
            console.error(chalk.red('Error loading config:', error.message));
            process.exit(1);
        }
    }

    parseProxy(proxyString) {
        try {
            let protocol, host, port;
            
            if (proxyString.includes('://')) {
                const url = new URL(proxyString);
                protocol = url.protocol.replace(':', '');
                host = url.hostname;
                port = url.port ? parseInt(url.port, 10) : null;
            } else {
                const parts = proxyString.split(':');
                if (parts.length === 3) {
                    [host, port, protocol] = parts;
                } else if (parts.length === 2) {
                    [host, port] = parts;
                    protocol = 'http';
                }
                port = port ? parseInt(port, 10) : null;
            }
    
            if (!protocol || !host || !port || isNaN(port)) {
                throw new Error(`Invalid proxy format: ${proxyString}`);
            }
    
            return { protocol: protocol.toLowerCase(), host, port };
        } catch (error) {
            console.error(chalk.red('Error parsing proxy:', proxyString, error.message));
            return null;
        }
    }
    

    loadProxies() {
        try {
            const data = fs.readFileSync('proxies.txt', 'utf8');
            return data.split('\n')
                .filter(line => line.trim())
                .map(proxy => this.parseProxy(proxy))
                .filter(proxy => proxy !== null);
        } catch (error) {
            console.log(chalk.yellow('No proxies found, using direct connection'));
            return [];
        }
    }

    getProxyAgent(proxy) {
        if (!proxy) return null;
        const proxyUrl = `${proxy.protocol}://${proxy.host}:${proxy.port}`;
        switch (proxy.protocol.toLowerCase()) {
            case 'socks4':
            case 'socks5':
                return new SocksProxyAgent(proxyUrl);
            case 'http':
            case 'https':
                return new HttpsProxyAgent(proxyUrl);
            default:
                return null;
        }
    }

    getNextProxy() {
        if (this.proxies.length === 0) return null;
        const proxy = this.proxies[this.proxyIndex];
        this.proxyIndex = (this.proxyIndex + 1) % this.proxies.length;
        return proxy;
    }

    handleMessage(ws, data, token) {
        const message = data.toString();
        console.log(chalk.cyan(`Received [${token.substring(0, 15)}...]:`, message));

        if (message.startsWith('0')) {
            // Handle handshake
            const handshake = JSON.parse(message.substring(1));
            this.pingInterval = handshake.pingInterval;
            this.pingTimeout = handshake.pingTimeout;
            
            // Send connection acknowledgment
            setTimeout(() => {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send('40{"sid":"' + handshake.sid + '"}');
                }
            }, 500);

        } else if (message.startsWith('2')) {
            // Respond to ping with pong
            if (ws.readyState === WebSocket.OPEN) {
                ws.send('3');
            }
        }
    }

    setupPingPong(ws, token) {
        let upMessageSent = false;
        let messageCount = 0;

        ws.on('message', (data) => {
            this.handleMessage(ws, data, token);
            messageCount++;

            // Send "up" message after receiving enough ping/pong cycles
            if (!upMessageSent && messageCount >= 10) {
                upMessageSent = true;
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send('42["up",{}]');
                }
            }
        });
    }

    createConnection(token) {
        const proxy = this.getNextProxy();
        const agent = this.getProxyAgent(proxy);
        
        const wsUrl = `wss://ws-v2.sparkchain.ai/socket.io/?token=${token}&device_id=${this.config.deviceId}&device_version=${this.deviceVersion}&EIO=4&transport=websocket`;

        const wsOptions = {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
                'Origin': 'chrome-extension://jlpniknnodfkbmbgkjelcailjljlecch',
                'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
                'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits'
            },
            agent: agent
        };

        const ws = new WebSocket(wsUrl, wsOptions);

        ws.on('open', () => {
            console.log(chalk.green(`Connected: ${token.substring(0, 15)}... ${proxy ? `via ${proxy.protocol} proxy` : 'direct'}`));
            this.connections.set(token, ws);
        });

        this.setupPingPong(ws, token);

        ws.on('error', (error) => {
            console.error(chalk.red(`Error [${token.substring(0, 15)}...]:`, error.message));
            this.reconnect(token);
        });

        ws.on('close', () => {
            console.log(chalk.yellow(`Disconnected: ${token.substring(0, 15)}...`));
            this.connections.delete(token);
            this.reconnect(token);
        });
    }

    reconnect(token) {
        console.log(chalk.yellow(`Attempting to reconnect: ${token.substring(0, 15)}...`));
        setTimeout(() => {
            if (!this.connections.has(token)) {
                this.createConnection(token);
            }
        }, 5000);
    }

    start() {
        // Display banner
        console.log(banner);
        
        if (!this.config.tokens || this.config.tokens.length === 0) {
            console.error(chalk.red('No tokens found in config.json'));
            return;
        }

        console.log(chalk.green(`Starting bot with ${this.config.tokens.length} accounts`));
        console.log(chalk.cyan(`Using ${this.proxies.length > 0 ? this.proxies.length + ' proxies' : 'direct connection'}`));

        for (const token of this.config.tokens) {
            this.createConnection(token);
        }
    }
}

// Start the bot
const bot = new WebSocketBot();
bot.start();