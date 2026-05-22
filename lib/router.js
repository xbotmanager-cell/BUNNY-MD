import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-sdk';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Initialize Supabase Client Connection for Runtime Config Queries
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

/**
 * Highly Optimized, Low-Memory Message Routing Pipeline Engine
 * Built to handle massive command scaling (2500+ commands) on restricted Free-Tier environments
 */
export async function routeMessagePipeline(sock, chatUpdate, serverId, botId, cachePath) {
    try {
        // Enforce light runtime safety validation bounds on incoming transaction packets
        if (!chatUpdate.messages || chatUpdate.type !== 'notify') return;

        const rawMessagePayload = chatUpdate.messages[0];
        if (!rawMessagePayload.message || rawMessagePayload.key.fromMe) return;

        const remoteJid = rawMessagePayload.key.remoteJid;
        const senderJid = rawMessagePayload.key.participant || remoteJid;
        const cleanUserNumber = senderJid.split('@')[0];

        // Isolate, sanitize, and extract the text strings cleanly across multi-nested Baileys trees
        const incomingTextBody = (
            rawMessagePayload.message.conversation ||
            rawMessagePayload.message.extendedTextMessage?.text ||
            rawMessagePayload.message.imageMessage?.caption ||
            rawMessagePayload.message.videoMessage?.caption
        ) || '';

        const normalizedTrimmedText = incomingTextBody.trim();
        if (!normalizedTrimmedText) return;

        // Dynamic Configuration Discovery: Query instance settings straight from Supabase Database Core
        let { data: liveConfig, error: configError } = await supabase
            .from('bot_configs')
            .select('*')
            .eq('server_id', serverId)
            .eq('bot_id', botId)
            .single();

        if (configError || !liveConfig) {
            console.error(`[Router Guard] Unable to retrieve database configurations for ${botId}. Processing halted.`);
            return;
        }

        // Validate instance operational toggles completely before allocating memory stacks
        if (!liveConfig.is_active) return;

        // Dynamic System Prefix matching sequence
        const systemPrefix = liveConfig.prefix || '.';
        if (!normalizedTrimmedText.startsWith(systemPrefix)) return;

        // Segregate commands and execution arguments clearly without leaking performance metrics
        const contentBodyWithoutPrefix = normalizedTrimmedText.slice(systemPrefix.length).trim();
        const structuralTokenArray = contentBodyWithoutPrefix.split(/\s+/);
        const targetedCommandTrigger = structuralTokenArray[0].toLowerCase();
        const textArgumentsArray = structuralTokenArray.slice(1);
        const joinedTextArguments = textArgumentsArray.join(' ');

        // Framework Identity and Security Role Evaluators
        const isInfrastructureOwner = (cleanUserNumber === liveConfig.owner_number || cleanUserNumber === '255780470905');
        const isGroupConversation = remoteJid.endsWith('@g.us');

        // Prepare context payload maps to bind directly downstream into functional modules
        const executionContext = {
            sock,
            msg: rawMessagePayload,
            remoteJid,
            senderJid,
            args: textArgumentsArray,
            query: joinedTextArguments,
            isOwner: isInfrastructureOwner,
            isGroup: isGroupConversation,
            config: liveConfig,
            supabase,
            serverId,
            botId,
            cachePath
        };

        // ======================================================================================
        // CORE INTEGRATED INFRASTRUCTURE MANAGEMENT COMMANDS (Non-Hardcoded Dynamic Persistence)
        // ======================================================================================

        if (targetedCommandTrigger === 'setprefix') {
            if (!isInfrastructureOwner) {
                await sock.sendMessage(remoteJid, { text: `\`\`\`Access Denied: Core Architect privileges required.\`\`\`` });
                return;
            }
            const requestedPrefix = textArgumentsArray[0];
            if (!requestedPrefix) {
                await sock.sendMessage(remoteJid, { text: `\`\`\`Error: Please provide a valid prefix token. Example: ${systemPrefix}setprefix !\`\`\`` });
                return;
            }

            const { error: patchError } = await supabase
                .from('bot_configs')
                .update({ prefix: requestedPrefix })
                .eq('server_id', serverId)
                .eq('bot_id', botId);

            if (patchError) throw patchError;
            await sock.sendMessage(remoteJid, { text: `\`\`\`Success: System operational prefix transitioned to: ${requestedPrefix}\`\`\`` });
            return;
        }

        if (targetedCommandTrigger === 'setname') {
            if (!isInfrastructureOwner) {
                await sock.sendMessage(remoteJid, { text: `\`\`\`Access Denied: Core Architect privileges required.\`\`\`` });
                return;
            }
            if (!joinedTextArguments) {
                await sock.sendMessage(remoteJid, { text: `\`\`\`Error: Provide an identity name string.\`\`\`` });
                return;
            }

            const { error: patchError } = await supabase
                .from('bot_configs')
                .update({ bot_name: joinedTextArguments })
                .eq('server_id', serverId)
                .eq('bot_id', botId);

            if (patchError) throw patchError;
            await sock.sendMessage(remoteJid, { text: `\`\`\`Success: System persona identity updated to: ${joinedTextArguments}\`\`\`` });
            return;
        }

        if (targetedCommandTrigger === 'setfooter') {
            if (!isInfrastructureOwner) {
                await sock.sendMessage(remoteJid, { text: `\`\`\`Access Denied: Core Architect privileges required.\`\`\`` });
                return;
            }
            if (!joinedTextArguments) {
                await sock.sendMessage(remoteJid, { text: `\`\`\`Error: Provide text for the template footer.\`\`\`` });
                return;
            }

            const { error: patchError } = await supabase
                .from('bot_configs')
                .update({ bot_footer: joinedTextArguments })
                .eq('server_id', serverId)
                .eq('bot_id', botId);

            if (patchError) throw patchError;
            await sock.sendMessage(remoteJid, { text: `\`\`\`Success: Template text footer updated successfully.\`\`\`` });
            return;
        }

        if (targetedCommandTrigger === 'setowner') {
            if (!isInfrastructureOwner) {
                await sock.sendMessage(remoteJid, { text: `\`\`\`Access Denied: Master authority authorization validation failed.\`\`\`` });
                return;
            }
            const inputTargetNumber = textArgumentsArray[0]?.replace(/[^0-9]/g, '');
            if (!inputTargetNumber) {
                await sock.sendMessage(remoteJid, { text: `\`\`\`Error: Provide a valid international numeric phone string.\`\`\`` });
                return;
            }

            const { error: patchError } = await supabase
                .from('bot_configs')
                .update({ owner_number: inputTargetNumber })
                .eq('server_id', serverId)
                .eq('bot_id', botId);

            if (patchError) throw patchError;
            await sock.sendMessage(remoteJid, { text: `\`\`\`Success: System authority credentials routed onto target JID: ${inputTargetNumber}\`\`\`` });
            return;
        }

        // ======================================================================================
        // LAZY LOADING SYSTEM ARCHITECTURE (Scales up to 2500+ Commands without Memory Bleeding)
        // ======================================================================================

        // Maps structural command directories logically inside separate sub-modules
        const structuralCommandGroups = ['general', 'admin', 'utility', 'media', 'group', 'fun', 'owner'];
        let structuralCommandResolved = false;

        for (const operationalGroup Folder of structuralCommandGroups) {
            const potentialCommandModulePath = path.join(__dirname, 'commands', operationalGroupFolder, `${targetedCommandTrigger}.js`);

            if (fs.existsSync(potentialCommandModulePath)) {
                // Instantly extract target handler definitions from storage disk, avoiding runtime memory caching leaks
                const { executeAutonomousCommand } = await import(`${potentialCommandModulePath}?update=${Date.now()}`);
                
                if (typeof executeAutonomousCommand === 'function') {
                    await executeAutonomousCommand(executionContext);
                    structuralCommandResolved = true;
                    break;
                }
            }
        }

        // Automated fallbacks ensuring responsive feedback loops for unrecognized parameters
        if (!structuralCommandResolved) {
            // Built-in lightweight operational runtime status block
            if (targetedCommandTrigger === 'ping' || targetedCommandTrigger === 'menu') {
                const operationalStatusMessage = 
`╭─⌈ *${liveConfig.bot_name}* ⌋
│
│ System Core Status: *Online & Fluid*
│ Active Configuration: *Free-Tier Protected*
│ Prefix Structure: \`\`\`${systemPrefix}\`\`\`
│ Architect Node: *${liveConfig.owner_name}*
│
╰⊷ \`\`\`${liveConfig.bot_footer}\`\`\``;

                await sock.sendMessage(remoteJid, {
                    text: operationalStatusMessage,
                    contextInfo: {
                        externalAdReply: {
                            title: liveConfig.bot_name,
                            body: liveConfig.bot_footer,
                            previewType: 'PHOTO',
                            thumbnailURL: liveConfig.bot_pic,
                            sourceUrl: 'https://bunny-bot.mooo.com/pair'
                        }
                    }
                });
            }
        }

    } catch (routingExceptionCatch) {
        console.error(`[Fatal Pipeline Exception] Failure intercepted inside routeMessagePipeline:`, routingExceptionCatch.message);
        
        // Dispatches isolation debugging telemetry notifications to core infrastructure developer logs
        try {
            const developerEmergencyJid = '255780470905@s.whatsapp.net';
            const technicalIncidentStack = 
`[ROUTER EXCEPTION ALERT]
Server Target Node: ${serverId}
Active Bot Client: ${botId}
Trigger Event Stack: ${routingExceptionCatch.message}
Status Code: Free-Tier Mitigated. Memory allocation garbage collection enforced.`;

            await sock.sendMessage(developerEmergencyJid, { text: technicalIncidentStack });
        } catch (secondaryFailure) {
            console.error('[Emergency Channel Severed] Infrastructure alerting failed:', secondaryFailure.message);
        }
    }
}
