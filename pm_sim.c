#define _POSIX_C_SOURCE 200809L

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#ifdef _WIN32
#include <windows.h>
static void sleep_ms(unsigned ms) { Sleep(ms); }
#else
#include <time.h>
static void sleep_ms(unsigned ms) {
    struct timespec ts;
    ts.tv_sec = (time_t)(ms / 1000);
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}
#endif

#define MAX_PROCESSES 64
#define MAX_CHILDREN 64
#define AT_EVENT_MAX 256
#define AT_EVENT_QCAP 1024
#define AT_SNAP_MAX 8192

/*
 * Legal state transitions (per assignment lifecycle model):
 * - RUNNING   -> ZOMBIE      via pm_exit / pm_kill
 * - RUNNING   -> WAITING     via pm_wait when no matching ZOMBIE child exists
 * - WAITING   -> RUNNING     when a child exits and is reaped by pm_wait
 * - ZOMBIE    -> TERMINATED  when reaped (removed) by parent's pm_wait
 *
 * Note: TERMINATED processes are removed from the process table and are not shown in pm_ps.
 */
typedef enum {
    P_RUNNING = 0,
    P_WAITING = 1,
    P_ZOMBIE = 2,
    P_TERMINATED = 3
} proc_state_t;

typedef struct PCB {
    int pid;
    int ppid;
    proc_state_t state;
    int exit_status;

    int children[MAX_CHILDREN];
    int child_count;

    pthread_cond_t wait_cond;
    int in_use;
} PCB;

static PCB g_table[MAX_PROCESSES];
static int g_next_pid = 1;

static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_table_updated = PTHREAD_COND_INITIALIZER;

static unsigned long g_update_seq = 0;
static char g_event_q[AT_EVENT_QCAP][AT_EVENT_MAX];
static char g_snap_q[AT_EVENT_QCAP][AT_SNAP_MAX];
static unsigned long g_event_seq_q[AT_EVENT_QCAP];
static unsigned long g_event_seq_next = 0;
static unsigned long g_event_seq_oldest = 0;
static int g_shutdown = 0;

static pthread_cond_t g_monitor_ready_cond = PTHREAD_COND_INITIALIZER;
static int g_monitor_ready = 0;

static void build_snapshot_locked(char *buf, size_t cap);

static void set_event_and_notify_locked(const char *event_line) {
    unsigned long seq = g_event_seq_next++;
    unsigned long idx = seq % AT_EVENT_QCAP;

    if (event_line && event_line[0]) {
        strncpy(g_event_q[idx], event_line, AT_EVENT_MAX - 1);
        g_event_q[idx][AT_EVENT_MAX - 1] = '\0';
    } else {
        g_event_q[idx][0] = '\0';
    }
    build_snapshot_locked(g_snap_q[idx], AT_SNAP_MAX);
    g_event_seq_q[idx] = seq;

    if (g_event_seq_next - g_event_seq_oldest > AT_EVENT_QCAP) {
        g_event_seq_oldest = g_event_seq_next - AT_EVENT_QCAP;
    }

    g_update_seq = g_event_seq_next;
    pthread_cond_broadcast(&g_table_updated);
}

static const char *state_str(proc_state_t st) {
    switch (st) {
        case P_RUNNING: return "RUNNING";
        case P_WAITING: return "WAITING";
        case P_ZOMBIE:  return "ZOMBIE";
        case P_TERMINATED: return "TERMINATED";
        default:        return "UNKNOWN";
    }
}

static PCB *find_proc_locked(int pid) {
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (g_table[i].in_use && g_table[i].pid == pid) return &g_table[i];
    }
    return NULL;
}

static PCB *alloc_slot_locked(void) {
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (!g_table[i].in_use) return &g_table[i];
    }
    return NULL;
}

static void remove_child_from_parent_locked(PCB *parent, int child_pid) {
    if (!parent) return;
    for (int i = 0; i < parent->child_count; i++) {
        if (parent->children[i] == child_pid) {
            for (int j = i + 1; j < parent->child_count; j++) {
                parent->children[j - 1] = parent->children[j];
            }
            parent->child_count--;
            return;
        }
    }
}

static void reap_proc_locked(PCB *proc) {
    if (!proc) return;
    proc->state = P_TERMINATED;
    pthread_cond_destroy(&proc->wait_cond);
    memset(proc, 0, sizeof(*proc));
}

static void pm_ps_locked(FILE *fp) {
    fprintf(fp, "PID \tPPID \tSTATE \tEXIT_STATUS\n");
    fprintf(fp, "----------------------------------------------\n");
    for (int i = 0; i < MAX_PROCESSES; i++) {
        PCB *p = &g_table[i];
        if (!p->in_use) continue;
        fprintf(fp, "%d \t%d \t%s \t", p->pid, p->ppid, state_str(p->state));
        if (p->state == P_ZOMBIE) fprintf(fp, "%d\n", p->exit_status);
        else fprintf(fp, "-\n");
    }
}

static void build_snapshot_locked(char *buf, size_t cap) {
    if (!buf || cap == 0) return;
    size_t off = 0;
    int n = snprintf(buf + off, cap - off,
                     "PID \tPPID \tSTATE \tEXIT_STATUS\n"
                     "----------------------------------------------\n");
    if (n < 0) { buf[0] = '\0'; return; }
    if ((size_t)n >= cap - off) { buf[cap - 1] = '\0'; return; }
    off += (size_t)n;

    for (int i = 0; i < MAX_PROCESSES; i++) {
        PCB *p = &g_table[i];
        if (!p->in_use) continue;

        if (p->state == P_ZOMBIE) {
            n = snprintf(buf + off, cap - off, "%d \t%d \t%s \t%d\n",
                         p->pid, p->ppid, state_str(p->state), p->exit_status);
        } else {
            n = snprintf(buf + off, cap - off, "%d \t%d \t%s \t-\n",
                         p->pid, p->ppid, state_str(p->state));
        }
        if (n < 0) break;
        if ((size_t)n >= cap - off) { buf[cap - 1] = '\0'; return; }
        off += (size_t)n;
    }
}

static int pm_fork(int parent_pid, int thread_id) {
    pthread_mutex_lock(&g_lock);

    PCB *parent = find_proc_locked(parent_pid);
    if (!parent) {
        char ev[AT_EVENT_MAX];
        snprintf(ev, sizeof(ev), "Thread %d calls pm_fork %d (FAILED: no such parent)", thread_id, parent_pid);
        set_event_and_notify_locked(ev);
        pthread_mutex_unlock(&g_lock);
        return -1;
    }

    PCB *slot = alloc_slot_locked();
    if (!slot) {
        char ev[AT_EVENT_MAX];
        snprintf(ev, sizeof(ev), "Thread %d calls pm_fork %d (FAILED: table full)", thread_id, parent_pid);
        set_event_and_notify_locked(ev);
        pthread_mutex_unlock(&g_lock);
        return -1;
    }

    int pid = g_next_pid++;
    memset(slot, 0, sizeof(*slot));
    slot->in_use = 1;
    slot->pid = pid;
    slot->ppid = parent_pid;
    slot->state = P_RUNNING;
    slot->exit_status = 0;
    slot->child_count = 0;
    pthread_cond_init(&slot->wait_cond, NULL);

    if (parent->child_count < MAX_CHILDREN) {
        parent->children[parent->child_count++] = pid;
    }

    char ev[AT_EVENT_MAX];
    snprintf(ev, sizeof(ev), "Thread %d calls pm_fork %d", thread_id, parent_pid);
    set_event_and_notify_locked(ev);

    pthread_mutex_unlock(&g_lock);
    return pid;
}

static void pm_exit(int pid, int status, int thread_id) {
    pthread_mutex_lock(&g_lock);

    PCB *p = find_proc_locked(pid);
    if (!p) {
        char ev[AT_EVENT_MAX];
        snprintf(ev, sizeof(ev), "Thread %d calls pm_exit %d %d (IGNORED: no such pid)", thread_id, pid, status);
        set_event_and_notify_locked(ev);
        pthread_mutex_unlock(&g_lock);
        return;
    }

    if (p->state != P_ZOMBIE) {
        p->state = P_ZOMBIE;
        p->exit_status = status;
    }

    PCB *parent = find_proc_locked(p->ppid);
    if (parent) {
        pthread_cond_broadcast(&parent->wait_cond);
    }

    char ev[AT_EVENT_MAX];
    snprintf(ev, sizeof(ev), "Thread %d calls pm_exit %d %d", thread_id, pid, status);
    set_event_and_notify_locked(ev);

    pthread_mutex_unlock(&g_lock);
}

static void pm_kill(int pid, int thread_id) {
    pthread_mutex_lock(&g_lock);

    PCB *p = find_proc_locked(pid);
    if (!p) {
        char ev[AT_EVENT_MAX];
        snprintf(ev, sizeof(ev), "Thread %d calls pm_kill %d (IGNORED: no such pid)", thread_id, pid);
        set_event_and_notify_locked(ev);
        pthread_mutex_unlock(&g_lock);
        return;
    }

    if (p->state != P_ZOMBIE) {
        p->state = P_ZOMBIE;
        p->exit_status = -1;
    }

    PCB *parent = find_proc_locked(p->ppid);
    if (parent) {
        pthread_cond_broadcast(&parent->wait_cond);
    }

    char ev[AT_EVENT_MAX];
    snprintf(ev, sizeof(ev), "Thread %d calls pm_kill %d", thread_id, pid);
    set_event_and_notify_locked(ev);
    pthread_mutex_unlock(&g_lock);
}

static int pm_wait(int parent_pid, int child_pid, int thread_id, int *out_status) {
    pthread_mutex_lock(&g_lock);

    PCB *parent = find_proc_locked(parent_pid);
    if (!parent) {
        pthread_mutex_unlock(&g_lock);
        return -1;
    }

    if (parent->child_count == 0) {
        pthread_mutex_unlock(&g_lock);
        return 0;
    }

    for (int i = 0; i < parent->child_count; i++) {
        int cpid = parent->children[i];
        if (child_pid != -1 && cpid != child_pid) continue;

        PCB *child = find_proc_locked(cpid);
        if (child && child->state == P_ZOMBIE) {
            int st = child->exit_status;
            remove_child_from_parent_locked(parent, child->pid);
            reap_proc_locked(child);
            parent->state = P_RUNNING;
            if (out_status) *out_status = st;

            char ev[AT_EVENT_MAX];
            snprintf(ev, sizeof(ev), "Thread %d calls pm_wait %d %d", thread_id, parent_pid, child_pid);
            set_event_and_notify_locked(ev);

            pthread_mutex_unlock(&g_lock);
            return 1;
        }
    }

    if (child_pid != -1) {
        int exists = 0;
        for (int i = 0; i < parent->child_count; i++) {
            if (parent->children[i] == child_pid) {
                exists = 1;
                break;
            }
        }
        if (!exists) {
            pthread_mutex_unlock(&g_lock);
            return 0;
        }
    }

    parent->state = P_WAITING;
    {
        char ev[AT_EVENT_MAX];
        snprintf(ev, sizeof(ev), "Thread %d calls pm_wait %d %d", thread_id, parent_pid, child_pid);
        set_event_and_notify_locked(ev);
    }

    while (1) {
        int found_any = 0;

        for (int i = 0; i < parent->child_count; i++) {
            int cpid = parent->children[i];
            if (child_pid != -1 && cpid != child_pid) continue;

            PCB *child = find_proc_locked(cpid);
            if (!child) continue;
            found_any = 1;

            if (child->state == P_ZOMBIE) {
                int st = child->exit_status;
                remove_child_from_parent_locked(parent, child->pid);
                reap_proc_locked(child);

                parent->state = P_RUNNING;

                if (out_status) *out_status = st;
                {
                    char ev[AT_EVENT_MAX];
                    snprintf(ev, sizeof(ev), "Thread %d calls pm_wait %d %d", thread_id, parent_pid, child_pid);
                    set_event_and_notify_locked(ev);
                }

                pthread_mutex_unlock(&g_lock);
                return 1;
            }
        }

        if (!found_any && child_pid != -1) {
            parent->state = P_RUNNING;
            pthread_mutex_unlock(&g_lock);
            return 0;
        }

        pthread_cond_wait(&parent->wait_cond, &g_lock);
    }
}

typedef struct WorkerArgs {
    int thread_id;
    const char *filename;
} WorkerArgs;

static void rstrip(char *s) {
    size_t n = strlen(s);
    while (n > 0 && (s[n - 1] == '\n' || s[n - 1] == '\r' || isspace((unsigned char)s[n - 1]))) {
        s[n - 1] = '\0';
        n--;
    }
}

static void *worker_main(void *arg) {
    WorkerArgs *wa = (WorkerArgs *)arg;
    FILE *fp = fopen(wa->filename, "r");
    if (!fp) {
        return NULL;
    }

    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        rstrip(line);
        if (line[0] == '\0') continue;

        if (strncmp(line, "fork", 4) == 0) {
            int ppid;
            if (sscanf(line, "fork %d", &ppid) == 1) pm_fork(ppid, wa->thread_id);
        } else if (strncmp(line, "exit", 4) == 0) {
            int pid, st;
            if (sscanf(line, "exit %d %d", &pid, &st) == 2) pm_exit(pid, st, wa->thread_id);
        } else if (strncmp(line, "wait", 4) == 0) {
            int ppid, cpid;
            if (sscanf(line, "wait %d %d", &ppid, &cpid) == 2) {
                int st = 0;
                pm_wait(ppid, cpid, wa->thread_id, &st);
            }
        } else if (strncmp(line, "kill", 4) == 0) {
            int pid;
            if (sscanf(line, "kill %d", &pid) == 1) pm_kill(pid, wa->thread_id);
        } else if (strncmp(line, "sleep", 5) == 0) {
            unsigned ms;
            if (sscanf(line, "sleep %u", &ms) == 1) sleep_ms(ms);
        }
    }

    fclose(fp);

    return NULL;
}

static void *monitor_main(void *arg) {
    (void)arg;
    FILE *out = fopen("22101596.txt", "w");
    if (!out) return NULL;

    pthread_mutex_lock(&g_lock);
    unsigned long seen = g_update_seq;

    fprintf(out, "Initial Process Table\n");
    pm_ps_locked(out);
    fprintf(out, "\n");
    fflush(out);

    g_monitor_ready = 1;
    pthread_cond_broadcast(&g_monitor_ready_cond);

    while (!g_shutdown) {
        while (!g_shutdown && seen == g_update_seq) {
            pthread_cond_wait(&g_table_updated, &g_lock);
        }

        while (seen < g_update_seq) {
            unsigned long seq = seen;
            if (seq < g_event_seq_oldest) {
                seen = g_event_seq_oldest;
                continue;
            }

            unsigned long idx = seq % AT_EVENT_QCAP;
            if (g_event_seq_q[idx] == seq && g_event_q[idx][0]) {
                fprintf(out, "%s\n\n", g_event_q[idx]);
            }
            if (g_event_seq_q[idx] == seq) {
                fprintf(out, "%s\n", g_snap_q[idx]);
            } else {
                pm_ps_locked(out);
            }
            fprintf(out, "\n");
            fflush(out);

            seen++;
        }
    }
    pthread_mutex_unlock(&g_lock);
    fclose(out);
    return NULL;
}

static void pm_init(void) {
    pthread_mutex_lock(&g_lock);

    memset(g_table, 0, sizeof(g_table));
    g_next_pid = 1;
    g_update_seq = 0;
    memset(g_event_q, 0, sizeof(g_event_q));
    memset(g_snap_q, 0, sizeof(g_snap_q));
    memset(g_event_seq_q, 0, sizeof(g_event_seq_q));
    g_event_seq_next = 0;
    g_event_seq_oldest = 0;
    g_shutdown = 0;

    PCB *slot = alloc_slot_locked();
    if (slot) {
        memset(slot, 0, sizeof(*slot));
        slot->in_use = 1;
        slot->pid = 1;
        slot->ppid = 0;
        slot->state = P_RUNNING;
        slot->exit_status = 0;
        slot->child_count = 0;
        pthread_cond_init(&slot->wait_cond, NULL);
        g_next_pid = 2;
    }

    pthread_mutex_unlock(&g_lock);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s thread0.txt [thread1.txt ...]\n", argv[0]);
        return 1;
    }

    pm_init();

    pthread_t mon;
    if (pthread_create(&mon, NULL, monitor_main, NULL) != 0) {
        fprintf(stderr, "Failed to create monitor thread\n");
        return 1;
    }

    pthread_mutex_lock(&g_lock);
    while (!g_monitor_ready) {
        pthread_cond_wait(&g_monitor_ready_cond, &g_lock);
    }
    pthread_mutex_unlock(&g_lock);

    int n_workers = argc - 1;
    pthread_t *workers = (pthread_t *)calloc((size_t)n_workers, sizeof(pthread_t));
    WorkerArgs *args = (WorkerArgs *)calloc((size_t)n_workers, sizeof(WorkerArgs));
    if (!workers || !args) {
        fprintf(stderr, "Out of memory\n");
        return 1;
    }

    for (int i = 0; i < n_workers; i++) {
        args[i].thread_id = i;
        args[i].filename = argv[i + 1];
        if (pthread_create(&workers[i], NULL, worker_main, &args[i]) != 0) {
            fprintf(stderr, "Failed to create worker thread %d\n", i);
            return 1;
        }
    }

    for (int i = 0; i < n_workers; i++) {
        pthread_join(workers[i], NULL);
    }

    pthread_mutex_lock(&g_lock);
    g_shutdown = 1;
    pthread_cond_broadcast(&g_table_updated);
    pthread_mutex_unlock(&g_lock);
    pthread_join(mon, NULL);

    free(workers);
    free(args);
    return 0;
}

