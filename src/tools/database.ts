import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { GraphQLClient } from "../graphqlClient.js";
import { text } from "../util/mcp.js";
import { wsUrlFromGraphQLEndpoint, connectWorkspaceSocket, joinWorkspace, loadDoc, pushDocUpdate } from "../ws.js";
import * as Y from "yjs";

export function registerDatabaseTools(server: McpServer, gql: GraphQLClient, defaults: { workspaceId?: string }) {
  function generateId(): string {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-';
    let id = '';
    for (let i = 0; i < 10; i++) id += chars.charAt(Math.floor(Math.random() * chars.length));
    return id;
  }

  async function getCookieAndEndpoint() {
    const endpoint = (gql as any).endpoint || process.env.AFFINE_BASE_URL + '/graphql';
    const headers = (gql as any).headers || {};
    const cookie = (gql as any).cookie || headers.Cookie || '';
    return { endpoint, cookie };
  }

  // ADD DATABASE ROW
  const addDatabaseRowHandler = async (parsed: {
    workspaceId?: string;
    docId: string;
    title: string;
    cells: Record<string, any>;
    userId?: string;
  }) => {
    const workspaceId = parsed.workspaceId || defaults.workspaceId;
    if (!workspaceId) throw new Error('workspaceId is required');

    const { endpoint, cookie } = await getCookieAndEndpoint();
    const wsUrl = wsUrlFromGraphQLEndpoint(endpoint);
    const socket = await connectWorkspaceSocket(wsUrl, cookie);

    try {
      await joinWorkspace(socket, workspaceId);

      // Load current document state
      const doc = new Y.Doc();
      const snapshot = await loadDoc(socket, workspaceId, parsed.docId);
      if (snapshot.missing) {
        Y.applyUpdate(doc, Buffer.from(snapshot.missing, 'base64'));
      }

      // Get state vector before changes
      const prevSV = Y.encodeStateVector(doc);
      const blocks = doc.getMap('blocks') as Y.Map<any>;

      // Find database block
      let dbBlock: any = null;
      let dbBlockId: string | null = null;
      for (const [blockId, block] of blocks) {
        const flavour = (block as any).get?.('sys:flavour');
        if (flavour && flavour.includes('database')) {
          dbBlock = block;
          dbBlockId = blockId;
          break;
        }
      }

      if (!dbBlock) {
        throw new Error('Database block not found in document');
      }

      // Generate row ID
      const rowId = generateId();

      // Create cells using proper nested YMaps
      const cells = dbBlock.get('prop:cells') as Y.Map<any>;
      const rowMap = new Y.Map();  // ✅ Creates local YMap!

      for (const [colId, value] of Object.entries(parsed.cells)) {
        const cellMap = new Y.Map();  // ✅ Creates local YMap!
        cellMap.set('columnId', colId);
        cellMap.set('value', value);
        rowMap.set(colId, cellMap);  // ✅ Nesting works!
      }

      cells.set(rowId, rowMap);  // ✅ Add row to cells map

      // Create paragraph block for row title
      const paraBlock = new Y.Map();
      const timestamp = Date.now();
      const userId = parsed.userId || 'baed6500-4936-4ea5-b9af-cca2602ac30e';

      paraBlock.set('sys:id', rowId);
      paraBlock.set('sys:flavour', 'affine:paragraph');
      paraBlock.set('sys:version', 1);
      paraBlock.set('prop:type', 'text');

      const paraText = new Y.Text();
      paraText.insert(0, parsed.title);
      paraBlock.set('prop:text', paraText);

      paraBlock.set('sys:children', new Y.Array());
      paraBlock.set('prop:collapsed', false);
      paraBlock.set('prop:meta:createdBy', userId);
      paraBlock.set('prop:meta:createdAt', timestamp);
      paraBlock.set('prop:meta:updatedBy', userId);
      paraBlock.set('prop:meta:updatedAt', timestamp);

      blocks.set(rowId, paraBlock);

      // Update database children
      const children = dbBlock.get('sys:children');
      if (children) {
        if (children.push) {
          // It's a YArray - just push directly
          children.push([rowId]);
        } else if (typeof children === 'string') {
          // It's a string - parse and update
          let childrenList: string[] = [];
          try {
            childrenList = JSON.parse(children.replace(/'/g, '"'));
          } catch {
            const childrenStr = children.replace(/[\[\]'"]/g, '');
            childrenList = childrenStr ? childrenStr.split(',').map((c: string) => c.trim()) : [];
          }
          childrenList.push(rowId);
          dbBlock.set('sys:children', JSON.stringify(childrenList));
        }
      }

      // Encode delta and push update
      const delta = Y.encodeStateAsUpdate(doc, prevSV);
      const deltaB64 = Buffer.from(delta).toString('base64');
      await pushDocUpdate(socket, workspaceId, parsed.docId, deltaB64);

      return text({
        success: true,
        rowId,
        title: parsed.title
      });

    } finally {
      socket.disconnect();
    }
  };

  server.registerTool(
    'add_database_row',
    {
      title: 'Add Database Row',
      description: 'Add a new row to an AFFiNE database with cell values',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Database document ID'),
        title: z.string().describe('Row title/text'),
        cells: z.record(z.any()).describe('Cell values as {columnId: value}'),
        userId: z.string().optional().describe('User ID for metadata'),
      },
    },
    addDatabaseRowHandler as any
  );

  server.registerTool(
    'affine_add_database_row',
    {
      title: 'Add Database Row',
      description: 'Add a new row to an AFFiNE database with cell values',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Database document ID'),
        title: z.string().describe('Row title/text'),
        cells: z.record(z.any()).describe('Cell values as {columnId: value}'),
        userId: z.string().optional().describe('User ID for metadata'),
      },
    },
    addDatabaseRowHandler as any
  );

  // UPDATE DATABASE CELL
  const updateDatabaseCellHandler = async (parsed: {
    workspaceId?: string;
    docId: string;
    rowId: string;
    columnId: string;
    value: any;
  }) => {
    const workspaceId = parsed.workspaceId || defaults.workspaceId;
    if (!workspaceId) throw new Error('workspaceId is required');

    const { endpoint, cookie } = await getCookieAndEndpoint();
    const wsUrl = wsUrlFromGraphQLEndpoint(endpoint);
    const socket = await connectWorkspaceSocket(wsUrl, cookie);

    try {
      await joinWorkspace(socket, workspaceId);

      // Load current document state
      const doc = new Y.Doc();
      const snapshot = await loadDoc(socket, workspaceId, parsed.docId);
      if (snapshot.missing) {
        Y.applyUpdate(doc, Buffer.from(snapshot.missing, 'base64'));
      }

      const prevSV = Y.encodeStateVector(doc);
      const blocks = doc.getMap('blocks') as Y.Map<any>;

      // Find database block
      let dbBlock: any = null;
      for (const [blockId, block] of blocks) {
        const flavour = (block as any).get?.('sys:flavour');
        if (flavour && flavour.includes('database')) {
          dbBlock = block;
          break;
        }
      }

      if (!dbBlock) {
        throw new Error('Database block not found');
      }

      const cells = dbBlock.get('prop:cells') as Y.Map<any>;
      const rowMap = cells.get(parsed.rowId);

      if (!rowMap) {
        throw new Error(`Row ${parsed.rowId} not found`);
      }

      const cellMap = rowMap.get(parsed.columnId);
      if (cellMap && cellMap.set) {
        // Update existing cell
        cellMap.set('value', parsed.value);
      } else {
        // Create new cell
        const newCell = new Y.Map();
        newCell.set('columnId', parsed.columnId);
        newCell.set('value', parsed.value);
        rowMap.set(parsed.columnId, newCell);
      }

      // Encode and push
      const delta = Y.encodeStateAsUpdate(doc, prevSV);
      const deltaB64 = Buffer.from(delta).toString('base64');
      await pushDocUpdate(socket, workspaceId, parsed.docId, deltaB64);

      return text({
        success: true,
        rowId: parsed.rowId,
        columnId: parsed.columnId,
        value: parsed.value
      });

    } finally {
      socket.disconnect();
    }
  };

  server.registerTool(
    'update_database_cell',
    {
      title: 'Update Database Cell',
      description: 'Update a cell value in an AFFiNE database row',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Database document ID'),
        rowId: z.string().describe('Row ID to update'),
        columnId: z.string().describe('Column ID'),
        value: z.any().describe('New cell value'),
      },
    },
    updateDatabaseCellHandler as any
  );

  server.registerTool(
    'affine_update_database_cell',
    {
      title: 'Update Database Cell',
      description: 'Update a cell value in an AFFiNE database row',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Database document ID'),
        rowId: z.string().describe('Row ID to update'),
        columnId: z.string().describe('Column ID'),
        value: z.any().describe('New cell value'),
      },
    },
    updateDatabaseCellHandler as any
  );

  // UPDATE ROW TITLE
  const updateRowTitleHandler = async (parsed: {
    workspaceId?: string;
    docId: string;
    rowId: string;
    title: string;
  }) => {
    const workspaceId = parsed.workspaceId || defaults.workspaceId;
    if (!workspaceId) throw new Error('workspaceId is required');

    const { endpoint, cookie } = await getCookieAndEndpoint();
    const wsUrl = wsUrlFromGraphQLEndpoint(endpoint);
    const socket = await connectWorkspaceSocket(wsUrl, cookie);

    try {
      await joinWorkspace(socket, workspaceId);

      // Load current document state
      const doc = new Y.Doc();
      const snapshot = await loadDoc(socket, workspaceId, parsed.docId);
      if (snapshot.missing) {
        Y.applyUpdate(doc, Buffer.from(snapshot.missing, 'base64'));
      }

      const prevSV = Y.encodeStateVector(doc);
      const blocks = doc.getMap('blocks') as Y.Map<any>;

      // Find the paragraph block for this row
      const paraBlock = blocks.get(parsed.rowId);
      if (!paraBlock) {
        throw new Error(`Row paragraph block not found: ${parsed.rowId}`);
      }

      // Update the text content
      const textProp = paraBlock.get('prop:text');
      if (textProp && textProp.delete && textProp.insert) {
        // It's a Y.Text - clear and insert new title
        const currentLength = textProp.length || textProp.toString().length;
        textProp.delete(0, currentLength);
        textProp.insert(0, parsed.title);
      } else {
        // Fallback: create new Y.Text
        const newText = new Y.Text();
        newText.insert(0, parsed.title);
        paraBlock.set('prop:text', newText);
      }

      // Update metadata
      const timestamp = Date.now();
      paraBlock.set('prop:meta:updatedAt', timestamp);

      // Encode and push
      const delta = Y.encodeStateAsUpdate(doc, prevSV);
      const deltaB64 = Buffer.from(delta).toString('base64');
      await pushDocUpdate(socket, workspaceId, parsed.docId, deltaB64);

      return text({
        success: true,
        rowId: parsed.rowId,
        title: parsed.title
      });

    } finally {
      socket.disconnect();
    }
  };

  server.registerTool(
    'update_row_title',
    {
      title: 'Update Database Row Title',
      description: 'Update the title of a database row',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Database document ID'),
        rowId: z.string().describe('Row ID to update'),
        title: z.string().describe('New title'),
      },
    },
    updateRowTitleHandler as any
  );

  server.registerTool(
    'affine_update_row_title',
    {
      title: 'Update Database Row Title',
      description: 'Update the title of a database row',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Database document ID'),
        rowId: z.string().describe('Row ID to update'),
        title: z.string().describe('New title'),
      },
    },
    updateRowTitleHandler as any
  );

  // UPDATE DOCUMENT CONTENT
  const updateDocContentHandler = async (parsed: {
    workspaceId?: string;
    docId: string;
    content: string;
    userId?: string;
  }) => {
    const workspaceId = parsed.workspaceId || defaults.workspaceId;
    if (!workspaceId) throw new Error('workspaceId is required');

    const { endpoint, cookie } = await getCookieAndEndpoint();
    const wsUrl = wsUrlFromGraphQLEndpoint(endpoint);
    const socket = await connectWorkspaceSocket(wsUrl, cookie);

    try {
      await joinWorkspace(socket, workspaceId);

      // Load current document state
      const doc = new Y.Doc();
      const snapshot = await loadDoc(socket, workspaceId, parsed.docId);
      if (snapshot.missing) {
        Y.applyUpdate(doc, Buffer.from(snapshot.missing, 'base64'));
      }

      const prevSV = Y.encodeStateVector(doc);
      const blocks = doc.getMap('blocks') as Y.Map<any>;

      // Find page block to get its children
      let pageBlock: any = null;
      let pageBlockId: string | null = null;
      for (const [blockId, block] of blocks) {
        const flavour = (block as any).get?.('sys:flavour');
        if (flavour === 'affine:page') {
          pageBlock = block;
          pageBlockId = blockId;
          break;
        }
      }

      if (!pageBlock) {
        throw new Error('Page block not found');
      }

      // Get existing children (paragraph blocks)
      const children = pageBlock.get('sys:children');
      const existingChildren: string[] = [];
      if (children) {
        if (children.toArray) {
          existingChildren.push(...children.toArray());
        } else if (typeof children === 'string') {
          try {
            existingChildren.push(...JSON.parse(children.replace(/'/g, '"')));
          } catch {
            const childrenStr = children.replace(/[\[\]'"]/g, '');
            if (childrenStr) existingChildren.push(...childrenStr.split(',').map((c: string) => c.trim()));
          }
        }
      }

      // Remove all existing paragraph blocks
      for (const childId of existingChildren) {
        const childBlock = blocks.get(childId);
        if (childBlock) {
          const flavour = childBlock.get?.('sys:flavour');
          if (flavour === 'affine:paragraph') {
            blocks.delete(childId);
          }
        }
      }

      // Split content into lines and create new paragraph blocks
      const lines = parsed.content.split('\n');
      const newChildren: string[] = existingChildren.filter(id => {
        const block = blocks.get(id);
        return block && block.get?.('sys:flavour') !== 'affine:paragraph';
      });

      const timestamp = Date.now();
      const userId = parsed.userId || 'baed6500-4936-4ea5-b9af-cca2602ac30e';

      for (const line of lines) {
        const paraId = generateId();
        const paraBlock = new Y.Map();

        paraBlock.set('sys:id', paraId);
        paraBlock.set('sys:flavour', 'affine:paragraph');
        paraBlock.set('sys:version', 1);
        paraBlock.set('prop:type', 'text');

        const paraText = new Y.Text();
        paraText.insert(0, line);
        paraBlock.set('prop:text', paraText);

        paraBlock.set('sys:children', new Y.Array());
        paraBlock.set('prop:collapsed', false);
        paraBlock.set('prop:meta:createdBy', userId);
        paraBlock.set('prop:meta:createdAt', timestamp);
        paraBlock.set('prop:meta:updatedBy', userId);
        paraBlock.set('prop:meta:updatedAt', timestamp);

        blocks.set(paraId, paraBlock);
        newChildren.push(paraId);
      }

      // Update page children
      if (children && children.push) {
        // Clear and repopulate Y.Array
        children.delete(0, children.length);
        for (const childId of newChildren) {
          children.push([childId]);
        }
      } else {
        pageBlock.set('sys:children', JSON.stringify(newChildren));
      }

      // Encode and push
      const delta = Y.encodeStateAsUpdate(doc, prevSV);
      const deltaB64 = Buffer.from(delta).toString('base64');
      await pushDocUpdate(socket, workspaceId, parsed.docId, deltaB64);

      return text({
        success: true,
        docId: parsed.docId,
        paragraphsCreated: lines.length
      });

    } finally {
      socket.disconnect();
    }
  };

  server.registerTool(
    'update_doc_content',
    {
      title: 'Update Document Content',
      description: 'Replace document content with new paragraphs',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Document ID'),
        content: z.string().describe('New content (lines separated by \\n)'),
        userId: z.string().optional().describe('User ID for metadata'),
      },
    },
    updateDocContentHandler as any
  );

  server.registerTool(
    'affine_update_doc_content',
    {
      title: 'Update Document Content',
      description: 'Replace document content with new paragraphs',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Document ID'),
        content: z.string().describe('New content (lines separated by \\n)'),
        userId: z.string().optional().describe('User ID for metadata'),
      },
    },
    updateDocContentHandler as any
  );

  // Helper: Load document and get title from page block
  async function getDocumentTitle(socket: any, workspaceId: string, docId: string): Promise<string> {
    try {
      const snapshot = await loadDoc(socket, workspaceId, docId);
      if (!snapshot.missing) return '';

      const doc = new Y.Doc();
      Y.applyUpdate(doc, Buffer.from(snapshot.missing, 'base64'));
      const blocks = doc.getMap('blocks');

      // Find page block and get title
      for (const [_, block] of blocks) {
        const flavour = (block as any).get?.('sys:flavour');
        if (flavour === 'affine:page') {
          const title = (block as any).get('prop:title');
          return title ? title.toString() : '';
        }
      }
      return '';
    } catch {
      return '';
    }
  }

  // READ DATABASE (columns + rows)
  const readDatabaseHandler = async (parsed: { workspaceId?: string; docId: string }) => {
    const workspaceId = parsed.workspaceId || defaults.workspaceId;
    if (!workspaceId) throw new Error('workspaceId is required');

    const { endpoint, cookie } = await getCookieAndEndpoint();
    const wsUrl = wsUrlFromGraphQLEndpoint(endpoint);
    const socket = await connectWorkspaceSocket(wsUrl, cookie);

    try {
      await joinWorkspace(socket, workspaceId);

      // Load current document state
      const doc = new Y.Doc();
      const snapshot = await loadDoc(socket, workspaceId, parsed.docId);
      if (snapshot.missing) {
        Y.applyUpdate(doc, Buffer.from(snapshot.missing, 'base64'));
      }

      const blocks = doc.getMap('blocks') as Y.Map<any>;

      // Find database block
      let dbBlock: any = null;
      let dbBlockId: string | null = null;
      for (const [blockId, block] of blocks) {
        const flavour = (block as any).get?.('sys:flavour');
        if (flavour && flavour.includes('database')) {
          dbBlock = block;
          dbBlockId = blockId;
          break;
        }
      }

      if (!dbBlock) {
        throw new Error('Database block not found in document');
      }

      // Extract columns
      const columnsArray = dbBlock.get('prop:columns') || [];
      const columns: any[] = [];
      for (const col of columnsArray) {
        if (typeof col === 'object' && col !== null) {
          columns.push({
            id: col.id || col.get?.('id'),
            name: col.name || col.get?.('name'),
            type: col.type || col.get?.('type'),
            data: col.data || col.get?.('data')
          });
        }
      }

      // Extract rows
      const cells = dbBlock.get('prop:cells');
      const children = dbBlock.get('sys:children');
      const rows: any[] = [];

      if (cells && children) {
        const childrenArray = Array.isArray(children) ? children :
                             (children.toArray ? children.toArray() : []);

        for (const rowId of childrenArray) {
          const rowCells = cells.get(rowId);
          if (!rowCells) continue;

          // Get row title and metadata from paragraph block
          const paraBlock = blocks.get(rowId);
          let rowTitle = '';
          let linkedDocId: string | null = null;
          let linkedDocTitle: string | null = null;
          let createdAt: number | null = null;
          let createdBy: string | null = null;
          let updatedAt: number | null = null;
          let updatedBy: string | null = null;

          if (paraBlock) {
            // Extract metadata
            createdAt = paraBlock.get('prop:meta:createdAt') || null;
            createdBy = paraBlock.get('prop:meta:createdBy') || null;
            updatedAt = paraBlock.get('prop:meta:updatedAt') || null;
            updatedBy = paraBlock.get('prop:meta:updatedBy') || null;

            const textProp = paraBlock.get('prop:text');
            if (textProp) {
              // Check for linked document reference
              const delta = (textProp as any).toDelta ? (textProp as any).toDelta() : null;
              if (delta) {
                for (const op of delta) {
                  if (op.attributes?.reference?.type === 'LinkedPage') {
                    linkedDocId = op.attributes.reference.pageId;
                    // Load linked document to get its title
                    if (linkedDocId) {
                      linkedDocTitle = await getDocumentTitle(socket, workspaceId, linkedDocId);
                      rowTitle = linkedDocTitle;
                    }
                    break;
                  }
                }
              }

              // Fallback to text content if no linked doc
              if (!linkedDocId) {
                rowTitle = textProp.toString();
              }
            }
          }

          // Parse cell values
          const cellValues: Record<string, any> = {};
          for (const [colId, cellData] of rowCells) {
            const column = columns.find(c => c.id === colId);
            let value = null;

            if (cellData && typeof cellData.get === 'function') {
              value = cellData.get('value');
            } else if (cellData && typeof cellData === 'object') {
              value = cellData.value;
            }

            cellValues[colId] = {
              columnName: column?.name || colId,
              columnType: column?.type || 'unknown',
              value
            };
          }

          const rowData: any = {
            id: rowId,
            title: rowTitle,
            cells: cellValues
          };

          // Include metadata if present
          if (createdAt) rowData.createdAt = createdAt;
          if (createdBy) rowData.createdBy = createdBy;
          if (updatedAt) rowData.updatedAt = updatedAt;
          if (updatedBy) rowData.updatedBy = updatedBy;

          // Include linked document info if present
          if (linkedDocId) {
            rowData.linkedDocId = linkedDocId;
            if (linkedDocTitle) {
              rowData.linkedDocTitle = linkedDocTitle;
            }
          }

          rows.push(rowData);
        }
      }

      return text({
        docId: parsed.docId,
        databaseId: dbBlockId,
        title: dbBlock.get('prop:title')?.toString() || 'Untitled',
        columns,
        rows,
        columnCount: columns.length,
        rowCount: rows.length
      });

    } finally {
      socket.disconnect();
    }
  };

  server.registerTool(
    'read_database',
    {
      title: 'Read Database',
      description: 'Read database structure with columns and rows',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Database document ID'),
      },
    },
    readDatabaseHandler as any
  );

  server.registerTool(
    'affine_read_database',
    {
      title: 'Read Database',
      description: 'Read database structure with columns and rows',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Database document ID'),
      },
    },
    readDatabaseHandler as any
  );

  // DELETE DATABASE ROW
  const deleteDatabaseRowHandler = async (parsed: {
    workspaceId?: string;
    docId: string;
    rowId: string;
  }) => {
    const workspaceId = parsed.workspaceId || defaults.workspaceId;
    if (!workspaceId) throw new Error('workspaceId is required');

    const { endpoint, cookie } = await getCookieAndEndpoint();
    const wsUrl = wsUrlFromGraphQLEndpoint(endpoint);
    const socket = await connectWorkspaceSocket(wsUrl, cookie);

    try {
      await joinWorkspace(socket, workspaceId);

      // Load current document state
      const doc = new Y.Doc();
      const snapshot = await loadDoc(socket, workspaceId, parsed.docId);
      if (snapshot.missing) {
        Y.applyUpdate(doc, Buffer.from(snapshot.missing, 'base64'));
      }

      const prevSV = Y.encodeStateVector(doc);
      const blocks = doc.getMap('blocks') as Y.Map<any>;

      // Find database block
      let dbBlock: any = null;
      for (const [blockId, block] of blocks) {
        const flavour = (block as any).get?.('sys:flavour');
        if (flavour && flavour.includes('database')) {
          dbBlock = block;
          break;
        }
      }

      if (!dbBlock) {
        throw new Error('Database block not found');
      }

      // Remove from children array
      const children = dbBlock.get('sys:children');
      if (children) {
        if (children.toArray) {
          const arr = children.toArray();
          const index = arr.indexOf(parsed.rowId);
          if (index >= 0) children.delete(index, 1);
        } else if (typeof children === 'string') {
          let childrenList = JSON.parse(children.replace(/'/g, '"'));
          childrenList = childrenList.filter((id: string) => id !== parsed.rowId);
          dbBlock.set('sys:children', JSON.stringify(childrenList));
        }
      }

      // Remove from cells map
      const cells = dbBlock.get('prop:cells');
      if (cells && cells.delete) {
        cells.delete(parsed.rowId);
      }

      // Remove paragraph block
      if (blocks.has(parsed.rowId)) {
        blocks.delete(parsed.rowId);
      }

      // Encode and push
      const delta = Y.encodeStateAsUpdate(doc, prevSV);
      const deltaB64 = Buffer.from(delta).toString('base64');
      await pushDocUpdate(socket, workspaceId, parsed.docId, deltaB64);

      return text({
        success: true,
        rowId: parsed.rowId,
        deleted: true
      });

    } finally {
      socket.disconnect();
    }
  };

  server.registerTool(
    'delete_database_row',
    {
      title: 'Delete Database Row',
      description: 'Delete a row from an AFFiNE database',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Database document ID'),
        rowId: z.string().describe('Row ID to delete'),
      },
    },
    deleteDatabaseRowHandler as any
  );

  server.registerTool(
    'affine_delete_database_row',
    {
      title: 'Delete Database Row',
      description: 'Delete a row from an AFFiNE database',
      inputSchema: {
        workspaceId: z.string().optional(),
        docId: z.string().describe('Database document ID'),
        rowId: z.string().describe('Row ID to delete'),
      },
    },
    deleteDatabaseRowHandler as any
  );

  // ADD DATABASE ROW WITH LINKED DOCUMENT
  const addDatabaseRowWithLinkedDocHandler = async (parsed: {
    workspaceId?: string;
    docId: string;
    documentTitle: string;
    documentContent: string;
    cells: Record<string, any>;
    userId?: string;
  }) => {
    const workspaceId = parsed.workspaceId || defaults.workspaceId;
    if (!workspaceId) throw new Error('workspaceId is required');

    const { endpoint, cookie } = await getCookieAndEndpoint();
    const wsUrl = wsUrlFromGraphQLEndpoint(endpoint);
    const socket = await connectWorkspaceSocket(wsUrl, cookie);

    try {
      await joinWorkspace(socket, workspaceId);

      // Generate IDs
      const linkedDocId = generateId() + generateId(); // Longer ID like Affine uses
      const rowId = generateId();
      const timestamp = Date.now();
      const userId = parsed.userId || 'baed6500-4936-4ea5-b9af-cca2602ac30e';

      // 1. Create the linked document
      const doc = new Y.Doc();
      const blocks = doc.getMap('blocks');

      // Page block
      const pageId = generateId() + generateId();
      const pageBlock = new Y.Map();
      pageBlock.set('sys:id', pageId);
      pageBlock.set('sys:flavour', 'affine:page');
      pageBlock.set('sys:version', 2);
      const pageChildren = new Y.Array();
      pageBlock.set('sys:children', pageChildren);
      const title = new Y.Text();
      title.insert(0, parsed.documentTitle);
      pageBlock.set('prop:title', title);
      blocks.set(pageId, pageBlock);

      // Surface block
      const surfaceId = generateId() + generateId();
      const surfaceBlock = new Y.Map();
      surfaceBlock.set('sys:id', surfaceId);
      surfaceBlock.set('sys:flavour', 'affine:surface');
      surfaceBlock.set('sys:version', 5);
      surfaceBlock.set('sys:parent', pageId);
      surfaceBlock.set('sys:children', new Y.Array());
      surfaceBlock.set('prop:elements', new Y.Map());
      blocks.set(surfaceId, surfaceBlock);
      pageChildren.push([surfaceId]);

      // Note block
      const noteId = generateId() + generateId();
      const noteBlock = new Y.Map();
      noteBlock.set('sys:id', noteId);
      noteBlock.set('sys:flavour', 'affine:note');
      noteBlock.set('sys:version', 1);
      noteBlock.set('sys:parent', pageId);
      const noteChildren = new Y.Array();
      noteBlock.set('sys:children', noteChildren);
      noteBlock.set('prop:xywh', '[0,0,800,95]');
      noteBlock.set('prop:background', new Y.Map());
      noteBlock.set('prop:index', 'a0');
      noteBlock.set('prop:lockedBySelf', false);
      noteBlock.set('prop:hidden', false);
      noteBlock.set('prop:displayMode', 'DocAndEdgeless');
      noteBlock.set('prop:edgeless', new Y.Map());
      blocks.set(noteId, noteBlock);
      pageChildren.push([noteId]);

      // Add content paragraph
      const paraId = generateId() + generateId();
      const paraBlock = new Y.Map();
      paraBlock.set('sys:id', paraId);
      paraBlock.set('sys:flavour', 'affine:paragraph');
      paraBlock.set('sys:version', 1);
      paraBlock.set('sys:parent', noteId);
      paraBlock.set('sys:children', new Y.Array());
      paraBlock.set('prop:type', 'text');
      const paraText = new Y.Text();
      paraText.insert(0, parsed.documentContent);
      paraBlock.set('prop:text', paraText);
      paraBlock.set('prop:collapsed', false);
      paraBlock.set('prop:meta:createdAt', timestamp);
      paraBlock.set('prop:meta:createdBy', userId);
      paraBlock.set('prop:meta:updatedAt', timestamp);
      paraBlock.set('prop:meta:updatedBy', userId);
      blocks.set(paraId, paraBlock);
      noteChildren.push([paraId]);

      // Add metadata
      const meta = doc.getMap('meta');
      meta.set('id', linkedDocId);
      meta.set('title', parsed.documentTitle);
      meta.set('createDate', timestamp);
      meta.set('tags', new Y.Array());

      // Push document
      const docUpdate = Y.encodeStateAsUpdate(doc);
      const docUpdateB64 = Buffer.from(docUpdate).toString('base64');
      await pushDocUpdate(socket, workspaceId, linkedDocId, docUpdateB64);

      // 2. Register document in workspace pages list
      const wsDoc = new Y.Doc();
      const snapshot = await loadDoc(socket, workspaceId, workspaceId);
      if (snapshot.missing) {
        Y.applyUpdate(wsDoc, Buffer.from(snapshot.missing, 'base64'));
      }
      const prevSV = Y.encodeStateVector(wsDoc);
      const wsMeta = wsDoc.getMap('meta');
      let pages = wsMeta.get('pages') as Y.Array<any> | undefined;
      if (!pages) {
        pages = new Y.Array();
        wsMeta.set('pages', pages);
      }
      const entry = new Y.Map();
      entry.set('id', linkedDocId);
      entry.set('title', parsed.documentTitle);
      entry.set('createDate', timestamp);
      entry.set('tags', new Y.Array());
      pages.push([entry as any]);
      const wsDelta = Y.encodeStateAsUpdate(wsDoc, prevSV);
      const wsDeltaB64 = Buffer.from(wsDelta).toString('base64');
      await pushDocUpdate(socket, workspaceId, workspaceId, wsDeltaB64);

      // 3. Create database row with link
      const dbSnapshot = await loadDoc(socket, workspaceId, parsed.docId);
      const dbDoc = new Y.Doc();
      if (dbSnapshot.missing) {
        Y.applyUpdate(dbDoc, Buffer.from(dbSnapshot.missing, 'base64'));
      }
      const dbPrevSV = Y.encodeStateVector(dbDoc);
      const dbBlocks = dbDoc.getMap('blocks') as Y.Map<any>;

      // Create row block with linked document reference
      const rowBlock = new Y.Map();
      rowBlock.set('sys:id', rowId);
      rowBlock.set('sys:flavour', 'affine:paragraph');
      rowBlock.set('sys:version', 1);
      rowBlock.set('sys:children', new Y.Array());
      rowBlock.set('prop:type', 'text');
      rowBlock.set('prop:collapsed', false);

      // Create Y.Text with reference delta
      const rowText = new Y.Text();
      rowText.applyDelta([{
        insert: ' ',
        attributes: {
          reference: {
            type: 'LinkedPage',
            pageId: linkedDocId
          }
        }
      }]);
      rowBlock.set('prop:text', rowText);
      rowBlock.set('prop:meta:createdAt', timestamp);
      rowBlock.set('prop:meta:createdBy', userId);
      rowBlock.set('prop:meta:updatedAt', timestamp);
      rowBlock.set('prop:meta:updatedBy', userId);

      dbBlocks.set(rowId, rowBlock);

      // Find database block and add row
      let dbBlock: Y.Map<any> | undefined;
      for (const [_, block] of dbBlocks.entries()) {
        const flavour = block.get('sys:flavour');
        if (flavour && flavour.includes('database')) {
          dbBlock = block;
          break;
        }
      }

      if (!dbBlock) {
        throw new Error('Database block not found in document');
      }

      // Update database children
      const children = dbBlock.get('sys:children');
      if (children) {
        if (children.push) {
          children.push([rowId]);
        } else if (typeof children === 'string') {
          let childrenList: string[] = [];
          try {
            childrenList = JSON.parse(children.replace(/'/g, '"'));
          } catch {
            const childrenStr = children.replace(/[\[\]'"]/g, '');
            childrenList = childrenStr ? childrenStr.split(',').map((c: string) => c.trim()) : [];
          }
          childrenList.push(rowId);
          dbBlock.set('sys:children', JSON.stringify(childrenList));
        }
      }

      // Set cell values (must use nested YMaps like original add_database_row)
      const cellsMap = dbBlock.get('prop:cells') as Y.Map<any>;
      if (cellsMap) {
        const rowCells = new Y.Map();
        for (const [columnId, value] of Object.entries(parsed.cells)) {
          const cellMap = new Y.Map();
          cellMap.set('columnId', columnId);
          cellMap.set('value', value);
          rowCells.set(columnId, cellMap);
        }
        cellsMap.set(rowId, rowCells);
      }

      // Push database update
      const dbDelta = Y.encodeStateAsUpdate(dbDoc, dbPrevSV);
      const dbDeltaB64 = Buffer.from(dbDelta).toString('base64');
      await pushDocUpdate(socket, workspaceId, parsed.docId, dbDeltaB64);

      return text({
        success: true,
        rowId,
        linkedDocId,
        title: parsed.documentTitle
      });

    } finally {
      socket.disconnect();
    }
  };

  server.registerTool(
    'add_database_row_with_linked_doc',
    {
      title: 'Add Database Row with Linked Document',
      description: 'Create a database row with a linked document containing rich content. The row will show as a clickable link to the document.',
      inputSchema: {
        workspaceId: z.string().optional().describe('Workspace ID (optional if default set)'),
        docId: z.string().describe('Database document ID'),
        documentTitle: z.string().describe('Title for the linked document'),
        documentContent: z.string().describe('Content for the linked document (can be multi-line with descriptions, lists, etc.)'),
        cells: z.record(z.any()).describe('Cell values as {columnId: value}. Use arrays for multi-select/member fields.'),
        userId: z.string().optional().describe('User ID for createdBy metadata'),
      },
    },
    addDatabaseRowWithLinkedDocHandler as any
  );

  server.registerTool(
    'affine_add_database_row_with_linked_doc',
    {
      title: 'Add Database Row with Linked Document',
      description: 'Create a database row with a linked document containing rich content. The row will show as a clickable link to the document.',
      inputSchema: {
        workspaceId: z.string().optional().describe('Workspace ID (optional if default set)'),
        docId: z.string().describe('Database document ID'),
        documentTitle: z.string().describe('Title for the linked document'),
        documentContent: z.string().describe('Content for the linked document (can be multi-line with descriptions, lists, etc.)'),
        cells: z.record(z.any()).describe('Cell values as {columnId: value}. Use arrays for multi-select/member fields.'),
        userId: z.string().optional().describe('User ID for createdBy metadata'),
      },
    },
    addDatabaseRowWithLinkedDocHandler as any
  );
}
