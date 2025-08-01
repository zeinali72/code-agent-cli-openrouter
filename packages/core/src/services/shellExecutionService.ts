/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { spawn } from 'child_process';
import { TextDecoder } from 'util';
import os from 'os';
import stripAnsi from 'strip-ansi';
import { getCachedEncodingForBuffer, detectEncodingFromBuffer } from '../utils/systemEncoding.js';
import { isBinary } from '../utils/textUtils.js';

const SIGKILL_TIMEOUT_MS = 200;

/** A structured result from a shell command execution. */
export interface ShellExecutionResult {
  /** The raw, unprocessed output buffer. */
  rawOutput: Buffer;
  /** The combined, decoded stdout and stderr as a string. */
  output: string;
  /** The decoded stdout as a string. */
  stdout: string;
  /** The decoded stderr as a string. */
  stderr: string;
  /** The process exit code, or null if terminated by a signal. */
  exitCode: number | null;
  /** The signal that terminated the process, if any. */
  signal: NodeJS.Signals | null;
  /** An error object if the process failed to spawn. */
  error: Error | null;
  /** A boolean indicating if the command was aborted by the user. */
  aborted: boolean;
  /** The process ID of the spawned shell. */
  pid: number | undefined;
}

/** A handle for an ongoing shell execution. */
export interface ShellExecutionHandle {
  /** The process ID of the spawned shell. */
  pid: number | undefined;
  /** A promise that resolves with the complete execution result. */
  result: Promise<ShellExecutionResult>;
}

/**
 * Describes a structured event emitted during shell command execution.
 */
export type ShellOutputEvent =
  | {
      /** The event contains a chunk of output data. */
      type: 'data';
      /** The stream from which the data originated. */
      stream: 'stdout' | 'stderr';
      /** The decoded string chunk. */
      chunk: string;
    }
  | {
      /** Signals that the output stream has been identified as binary. */
      type: 'binary_detected';
    }
  | {
      /** Provides progress updates for a binary stream. */
      type: 'binary_progress';
      /** The total number of bytes received so far. */
      bytesReceived: number;
    };

/**
 * A centralized service for executing shell commands with robust process
 * management, cross-platform compatibility, and streaming output capabilities.
 *
 */
export class ShellExecutionService {
  /**
   * Executes a shell command using `spawn`, capturing all output and lifecycle events.
   *
   * @param commandToExecute The exact command string to run.
   * @param cwd The working directory to execute the command in.
   * @param onOutputEvent A callback for streaming structured events about the execution, including data chunks and status updates.
   * @param abortSignal An AbortSignal to terminate the process and its children.
   * @returns An object containing the process ID (pid) and a promise that
   *          resolves with the complete execution result.
   */
  static execute(
    commandToExecute: string,
    cwd: string,
    onOutputEvent: (event: ShellOutputEvent) => void,
    abortSignal: AbortSignal,
  ): ShellExecutionHandle {
    const isWindows = os.platform() === 'win32';
    
    let shell: string;
    let shellArgs: string[];
    let spawnOptions: any;

    if (isWindows) {
      // For Windows, use cmd.exe with proper encoding handling
      shell = 'cmd.exe';
      
      // Attempt to set UTF-8 code page before running the command to handle
      // multi-byte environments better, then run the actual command
      const encodingCommand = `chcp 65001 >nul 2>&1 & ${commandToExecute}`;
      shellArgs = ['/c', encodingCommand];
      
      spawnOptions = {
        cwd,
        stdio: ['ignore', 'pipe', 'pipe'],
        detached: false, // Don't use detached on Windows for better process control
        env: {
          ...process.env,
          LLXPRT_CLI: '1',
          // Force UTF-8 code page for better encoding support
          'PYTHONIOENCODING': 'utf-8',
        },
        // On Windows, set the windowsVerbatimArguments to prevent Node.js from 
        // over-escaping command arguments which can cause the excessive quoting issue
        windowsVerbatimArguments: false,
      };
    } else {
      // For Unix-like systems, use bash as before
      shell = 'bash';
      shellArgs = ['-c', commandToExecute];
      
      spawnOptions = {
        cwd,
        stdio: ['ignore', 'pipe', 'pipe'],
        detached: true, // Use process groups on non-Windows for robust killing
        env: {
          ...process.env,
          LLXPRT_CLI: '1',
        },
      };
    }

    const child = spawn(shell, shellArgs, spawnOptions);

    const result = new Promise<ShellExecutionResult>((resolve) => {
      // Use decoders to handle multi-byte characters safely (for streaming output).
      let stdoutDecoder: TextDecoder | null = null;
      let stderrDecoder: TextDecoder | null = null;

      let stdout = '';
      let stderr = '';
      const outputChunks: Buffer[] = [];
      let error: Error | null = null;
      let exited = false;

      let isStreamingRawContent = true;
      const MAX_SNIFF_SIZE = 4096;
      let sniffedBytes = 0;

      const handleOutput = (data: Buffer, stream: 'stdout' | 'stderr') => {
        if (!stdoutDecoder || !stderrDecoder) {
          let encoding = getCachedEncodingForBuffer(data);
          
          // Special handling for Windows multi-byte environments
          if (os.platform() === 'win32' && (!encoding || encoding === 'utf-8')) {
            // Try to detect if we're in a multi-byte environment
            // by checking for common Japanese/Asian encodings
            const detectedEncoding = detectEncodingFromBuffer(data);
            if (detectedEncoding && 
                (detectedEncoding.includes('shift') || 
                 detectedEncoding.includes('932') ||
                 detectedEncoding.includes('sjis'))) {
              encoding = 'shift_jis';
            }
          }
          
          try {
            stdoutDecoder = new TextDecoder(encoding);
            stderrDecoder = new TextDecoder(encoding);
          } catch (error) {
            // If the encoding is not supported, fall back to utf-8.
            // This can happen on some platforms for certain encodings.
            console.warn(`Unsupported encoding '${encoding}', falling back to utf-8:`, error);
            stdoutDecoder = new TextDecoder('utf-8');
            stderrDecoder = new TextDecoder('utf-8');
          }
        }

        outputChunks.push(data);

        // Binary detection logic. This only runs until we've made a determination.
        if (isStreamingRawContent && sniffedBytes < MAX_SNIFF_SIZE) {
          const sniffBuffer = Buffer.concat(outputChunks.slice(0, 20));
          sniffedBytes = sniffBuffer.length;

          if (isBinary(sniffBuffer)) {
            // Change state to stop streaming raw content.
            isStreamingRawContent = false;
            onOutputEvent({ type: 'binary_detected' });
          }
        }

        const decodedChunk =
          stream === 'stdout'
            ? stdoutDecoder.decode(data, { stream: true })
            : stderrDecoder.decode(data, { stream: true });
        const strippedChunk = stripAnsi(decodedChunk);

        if (stream === 'stdout') {
          stdout += strippedChunk;
        } else {
          stderr += strippedChunk;
        }

        if (isStreamingRawContent) {
          onOutputEvent({ type: 'data', stream, chunk: strippedChunk });
        } else {
          const totalBytes = outputChunks.reduce(
            (sum, chunk) => sum + chunk.length,
            0,
          );
          onOutputEvent({ type: 'binary_progress', bytesReceived: totalBytes });
        }
      };

      child.stdout.on('data', (data) => handleOutput(data, 'stdout'));
      child.stderr.on('data', (data) => handleOutput(data, 'stderr'));
      child.on('error', (err) => {
        error = err;
      });

      const abortHandler = async () => {
        if (child.pid && !exited) {
          if (isWindows) {
            spawn('taskkill', ['/pid', child.pid.toString(), '/f', '/t']);
          } else {
            try {
              // Kill the entire process group (negative PID).
              // SIGTERM first, then SIGKILL if it doesn't die.
              process.kill(-child.pid, 'SIGTERM');
              await new Promise((res) => setTimeout(res, SIGKILL_TIMEOUT_MS));
              if (!exited) {
                process.kill(-child.pid, 'SIGKILL');
              }
            } catch (_e) {
              // Fall back to killing just the main process if group kill fails.
              if (!exited) child.kill('SIGKILL');
            }
          }
        }
      };

      abortSignal.addEventListener('abort', abortHandler, { once: true });

      child.on('exit', (code, signal) => {
        exited = true;
        abortSignal.removeEventListener('abort', abortHandler);

        if (stdoutDecoder) {
          stdout += stripAnsi(stdoutDecoder.decode());
        }
        if (stderrDecoder) {
          stderr += stripAnsi(stderrDecoder.decode());
        }

        const finalBuffer = Buffer.concat(outputChunks);

        resolve({
          rawOutput: finalBuffer,
          output: stdout + (stderr ? `\n${stderr}` : ''),
          stdout,
          stderr,
          exitCode: code,
          signal,
          error,
          aborted: abortSignal.aborted,
          pid: child.pid,
        });
      });
    });

    return { pid: child.pid, result };
  }
}
