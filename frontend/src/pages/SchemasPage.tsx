import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';
import { registerSchema, deleteSchema, getSchemas, listSchemaSubjects } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger,
} from '@/components/ui/dialog';
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import {
  Tabs, TabsContent, TabsList, TabsTrigger,
} from '@/components/ui/tabs';
import { BookOpen, Plus, Eye, Trash2, Copy, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';

const typeBadges: Record<string, 'default' | 'secondary' | 'success'> = {
  JSON: 'success',
  Avro: 'default',
  Protobuf: 'secondary',
};

const schemaSchema = z.object({
  subject: z.string().min(1, 'Subject is required').max(256, 'Subject is too long').regex(/^[a-zA-Z0-9._-]+$/, 'Only letters, numbers, dots, hyphens, and underscores'),
  type: z.enum(['JSON', 'Avro', 'Protobuf']),
  definition: z.string().min(1, 'Schema definition is required').max(1_048_576, 'Definition is too large'),
});

type SchemaFormValues = z.infer<typeof schemaSchema>;

async function loadSubjects(): Promise<string[]> {
  try {
    return await listSchemaSubjects();
  } catch {
    return [];
  }
}

export function SchemasPage() {
  const queryClient = useQueryClient();
  const [registerOpen, setRegisterOpen] = useState(false);
  const [subject, setSubject] = useState('');
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);

  const form = useForm<SchemaFormValues>({
    resolver: zodResolver(schemaSchema),
    defaultValues: { subject: '', type: 'JSON', definition: '' },
  });

  const { data: subjects, isLoading } = useQuery({
    queryKey: ['schema-subjects'],
    queryFn: loadSubjects,
    refetchInterval: 15_000,
  });

  const { data: schemas } = useQuery({
    queryKey: ['schema-detail', subject],
    queryFn: () => getSchemas(subject),
    enabled: !!subject,
  });

  const registerMutation = useMutation({
    mutationFn: (data: SchemaFormValues) => registerSchema(data.subject, data.type, data.definition),
    onSuccess: () => {
      toast.success(`Schema registered for "${form.getValues('subject')}"`);
      setRegisterOpen(false);
      form.reset();
      queryClient.invalidateQueries({ queryKey: ['schema-subjects'] });
    },
    onError: (err: { message?: string }) => {
      toast.error(err.message ?? 'Failed to register schema');
    },
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteSchema(deleteTarget!),
    onSuccess: () => {
      toast.success(`Schema "${deleteTarget}" deleted`);
      setDeleteTarget(null);
      setSubject('');
      queryClient.invalidateQueries({ queryKey: ['schema-subjects'] });
    },
    onError: (err: { message?: string }) => {
      toast.error(err.message ?? 'Failed to delete schema');
    },
  });

  if (isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-48" />
        <Card><CardContent className="py-4"><Skeleton className="h-12 w-full mb-2" /></CardContent></Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-foreground">Schemas</h1>
          <p className="text-sm text-text-secondary">Schema Registry — Avro, Protobuf, JSON Schema</p>
        </div>
        <Dialog open={registerOpen} onOpenChange={(open) => { setRegisterOpen(open); if (open) form.reset(); }}>
          <DialogTrigger asChild>
            <Button size="sm">
              <Plus className="mr-2 h-4 w-4" />
              Register Schema
            </Button>
          </DialogTrigger>
          <DialogContent className="max-w-2xl w-full h-full sm:h-auto sm:rounded-lg">
            <DialogHeader>
              <DialogTitle>Register Schema</DialogTitle>
              <DialogDescription>Register a new schema for a subject</DialogDescription>
            </DialogHeader>
            <form onSubmit={form.handleSubmit((data) => registerMutation.mutate(data))} className="space-y-4">
              <div>
                <Label htmlFor="schema-subject">Subject</Label>
                <Input
                  id="schema-subject"
                  {...form.register('subject')}
                  placeholder="my-topic-value"
                  aria-invalid={!!form.formState.errors.subject}
                />
                {form.formState.errors.subject && (
                  <p className="text-xs text-error mt-1">{form.formState.errors.subject.message}</p>
                )}
              </div>
              <div>
                <Label htmlFor="schema-type">Type</Label>
                <Select
                  value={form.watch('type')}
                  onValueChange={(v) => form.setValue('type', v as SchemaFormValues['type'])}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="JSON">JSON Schema</SelectItem>
                    <SelectItem value="Avro">Avro</SelectItem>
                    <SelectItem value="Protobuf">Protobuf</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label htmlFor="schema-def">Schema Definition</Label>
                <Textarea
                  id="schema-def"
                  {...form.register('definition')}
                  placeholder='{"type": "object", "properties": {...}}'
                  className="min-h-[160px] font-mono"
                  aria-invalid={!!form.formState.errors.definition}
                />
                {form.formState.errors.definition && (
                  <p className="text-xs text-error mt-1">{form.formState.errors.definition.message}</p>
                )}
              </div>
              <DialogFooter>
                <Button variant="outline" type="button" onClick={() => setRegisterOpen(false)}>Cancel</Button>
                <Button
                  type="submit"
                  disabled={!form.formState.isValid || registerMutation.isPending}
                >
                  {registerMutation.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                  Register
                </Button>
              </DialogFooter>
            </form>
          </DialogContent>
        </Dialog>
      </div>

      {/* Subjects List */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <BookOpen className="h-5 w-5 text-text-muted" />
            {subjects?.length ?? 0} Subjects
          </CardTitle>
          <CardDescription>Registered schema subjects and versions</CardDescription>
        </CardHeader>
        <CardContent>
          {(!subjects || subjects.length === 0) ? (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <BookOpen className="h-12 w-12 text-text-muted mb-4" />
              <h3 className="text-lg font-medium text-foreground">No schemas registered</h3>
              <p className="text-sm text-text-secondary mt-1">
                Register a schema to enable message validation
              </p>
              <Button className="mt-4" size="sm" onClick={() => setRegisterOpen(true)}>
                <Plus className="mr-2 h-4 w-4" />
                Register Schema
              </Button>
            </div>
          ) : (
            <>
              {/* Mobile card view */}
              <div className="space-y-2 sm:hidden">
                {subjects.map((s) => (
                  <div key={s} className="rounded-lg border border-border bg-background p-4 space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="font-mono text-sm font-medium">{s}</span>
                      <Badge variant="secondary">Schema</Badge>
                    </div>
                    <div className="flex gap-1 pt-1 border-t border-border">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-11 w-full min-h-[44px]"
                        onClick={() => setSubject(s)}
                        aria-label={`View schema ${s}`}
                      >
                        <Eye className="h-4 w-4 mr-2" />
                        View
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-11 w-full min-h-[44px] text-error hover:text-error"
                        onClick={() => setDeleteTarget(s)}
                        aria-label={`Delete schema ${s}`}
                      >
                        <Trash2 className="h-4 w-4 mr-2" />
                        Delete
                      </Button>
                    </div>
                  </div>
                ))}
              </div>

              {/* Desktop table */}
              <Table className="hidden sm:table">
                <TableHeader>
                  <TableRow>
                    <TableHead>Subject</TableHead>
                    <TableHead>Versions</TableHead>
                    <TableHead className="w-32">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {subjects.map((s) => (
                    <TableRow key={s}>
                      <TableCell className="font-mono text-sm">{s}</TableCell>
                      <TableCell>
                        <Badge variant="secondary">Schema</Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex gap-1">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => setSubject(s)}
                            aria-label={`View schema ${s}`}
                          >
                            <Eye className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="text-error hover:text-error"
                            onClick={() => setDeleteTarget(s)}
                            aria-label={`Delete schema ${s}`}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </>
          )}
        </CardContent>
      </Card>

      {/* Schema Detail */}
      {subject && schemas && (
        <Card>
          <CardHeader>
            <CardTitle>{subject}</CardTitle>
            <CardDescription>{schemas.length} version(s)</CardDescription>
          </CardHeader>
          <CardContent>
            <Tabs defaultValue="0">
              <TabsList>
                {schemas.map((s, i) => (
                  <TabsTrigger key={s.version} value={String(i)}>v{s.version}</TabsTrigger>
                ))}
              </TabsList>
              {schemas.map((s, i) => (
                <TabsContent key={s.version} value={String(i)} className="mt-4">
                  <div className="flex items-center gap-2 mb-3">
                    <Badge variant={typeBadges[s.type] ?? 'secondary'}>{s.type}</Badge>
                    <span className="text-xs text-text-muted">ID: {s.id}</span>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => {
                        navigator.clipboard.writeText(s.schema);
                        toast.success('Schema copied to clipboard');
                      }}
                    >
                      <Copy className="h-3 w-3 mr-1" /> Copy
                    </Button>
                  </div>
                  <pre className="rounded-lg border border-border bg-background p-4 text-xs overflow-auto max-h-96 font-mono">
                    {s.schema}
                  </pre>
                </TabsContent>
              ))}
            </Tabs>
          </CardContent>
        </Card>
      )}

      {/* Delete Confirmation */}
      <AlertDialog open={!!deleteTarget} onOpenChange={() => setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Schema</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete "{deleteTarget}"? This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => deleteMutation.mutate()}
              className="bg-error hover:bg-error/90"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
