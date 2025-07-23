/**
 * This is the simplest possible implementation: a "Standalone Worker".
 * It is designed to run on a single pod ONLY and has no external dependencies
 * like Redis or RabbitMQ for locking or queueing.
 *
 * - It uses an in-memory boolean flag to prevent cron job overlaps.
 * - It uses an in-memory library ('p-limit') to process files concurrently.
 */

// Import a lightweight in-memory concurrency library
import pLimit from 'p-limit';

// --- 1. Scheduler with In-Memory Lock ---

@Injectable()
class SchedulerService {
  // In-memory flag to prevent overlapping runs. THIS ONLY WORKS FOR A SINGLE POD.
  private isScanRunning = false;

  constructor(private readonly sharepointScanner: SharePointScannerService) {}

  @Cron('*/15 * * * *')
  async handleCron() {
    // Check the in-memory lock
    if (this.isScanRunning) {
      console.log('Scan skipped, a previous scan is still in progress.');
      return;
    }

    try {
      // Set the lock
      this.isScanRunning = true;
      console.log('Starting SharePoint scan...');
      await this.sharepointScanner.startScanAndProcess();
    } catch (error) {
      console.error('An error occurred during the scan.', error);
    } finally {
      // ALWAYS release the lock
      this.isScanRunning = false;
      console.log('Scan finished.');
    }
  }
}

// --- 2. Scanner and In-Process Processing ---

@Injectable()
class SharePointScannerService {
  constructor(
    private readonly graphApiService: GraphApiService,
    private readonly pipelineService: PipelineService,
    private readonly configService: ConfigService,
  ) {}

  async startScanAndProcess() {
    const sites = await this.graphApiService.getConfiguredSites();
    const allFilesToProcess = [];

    for (const site of sites) {
      if (site.isMarkedForDeletion) {
        // In this simple model, we process deletions immediately
        await this.pipelineService.deleteSitePipeline(site.id);
      } else {
        const files = await this.graphApiService.getFilesToSync(site.id);
        allFilesToProcess.push(...files);
      }
    }

    // Process all identified files using an in-memory concurrency limiter
    const concurrency = this.configService.get('PROCESSING_CONCURRENCY', 4);
    const limit = pLimit(concurrency);

    const processingPromises = allFilesToProcess.map(file => {
      return limit(() => this.pipelineService.processFilePipeline(file));
    });

    // Wait for all files to be processed
    await Promise.all(processingPromises);
  }
}


// --- 3. The Processing Pipeline Itself (No changes needed) ---

@Injectable()
class PipelineService {
  constructor(
    private readonly graphApiService: GraphApiService,
    private readonly fileDiffService: FileDiffService,
    private readonly blobStorageService: BlobStorageService,
    private readonly knowledgeBaseApi: KnowledgeBaseApi,
  ) {}

  async processFilePipeline(file: any) {
    try {
      const fileDetails = await this.graphApiService.getFileDetails(file.id);
      const processedContent = this.processContent(fileDetails.content);
      const diffResult = await this.fileDiffService.diff(file.id, processedContent);
      if (diffResult.hasChanged) {
        const storageUrl = await this.blobStorageService.upload(processedContent);
        await this.knowledgeBaseApi.ingest(file.id, storageUrl);
      }
      console.log(`Successfully processed file: ${file.id}`);
    } catch (error) {
      console.error(`Error processing file ${file.id}:`, error);
      // In this simple model, an error in one file does not stop others,
      // but it will be logged. There are no automatic retries.
    }
  }

  async deleteSitePipeline(siteId: string) {
    try {
      await this.fileDiffService.signalDeletionForSite(siteId);
      console.log(`Successfully signaled deletion for site: ${siteId}`);
    } catch (error) {
      console.error(`Failed to signal deletion for site ${siteId}`, error);
    }
  }

  private processContent(content: any): any {
    return content;
  }
}


// --- Helper Service Stubs (for context) ---

// Added for handling environment variables
class ConfigService {
  get(key: string, defaultValue: any): any {
    return process.env[key] || defaultValue;
  }
}

class GraphApiService {
  /* ... methods to interact with MS Graph API ... */
}
class FileDiffService {
  /* ... methods to interact with the file-diff endpoint ... */
}
class BlobStorageService {
  /* ... methods to upload to Azure Blob Storage ... */
}
class KnowledgeBaseApi {
  /* ... methods to interact with the Unique KB API ... */
}
