#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <windows.h>
#include <io.h>
#include <fcntl.h>
#include <time.h>

#define BUFFER_SIZE 4096
#define HISTORY_FILE "history.txt"
#define SCHEDULE_FILE "schedule.txt"
#define MAX_ENTRIES 100      // 最大100件の履歴を処理
#define MAX_REPLACE_LEN MAX_PATH

// グローバル変数：コピー完了後にコピー元を削除するかどうか
int g_deleteSource = 0;

// ログ用ミューテックス
HANDLE g_logMutex;

// コピータスクを表す構造体
typedef struct _CopyTask {
    char src[MAX_PATH];
    char dest[MAX_PATH];
    unsigned long long folder_size;
    int task_id;
    char replace_from[MAX_REPLACE_LEN]; // 置換前文字列（ファイル置換の場合）
    char replace_to[MAX_REPLACE_LEN];   // 置換後文字列（ファイル置換の場合）
    char replace_option[MAX_REPLACE_LEN]; // オプション："d"ならフォルダ名置換
} CopyTask;

// ----- ログ出力用関数 -----
// ログメッセージを "log.txt" に追記（スレッドセーフ）
void log_message(const char *format, ...) {
    WaitForSingleObject(g_logMutex, INFINITE);
    FILE *fp = fopen("log.txt", "a");
    if (fp) {
        va_list args;
        va_start(args, format);
        vfprintf(fp, format, args);
        va_end(args);
        fclose(fp);
    }
    ReleaseMutex(g_logMutex);
}

// ----- ユーティリティ関数 -----
// 前後の空白を削除する関数
void trim(char *str) {
    char *start = str;
    while (isspace((unsigned char)*start))
        start++;
    if (start != str)
        memmove(str, start, strlen(start) + 1);
    int len = strlen(str);
    while (len > 0 && isspace((unsigned char)str[len - 1])) {
        str[len - 1] = '\0';
        len--;
    }
}

// サイズを適切な単位（B, KB, MB, GB, TB, PB）でフォーマットする関数（1024単位）
char* format_size(unsigned long long size, char *buf, size_t bufsize) {
    const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    int unit_index = 0;
    double dsize = (double) size;
    while (dsize >= 1024 && unit_index < 5) {
        dsize /= 1024;
        unit_index++;
    }
    snprintf(buf, bufsize, "%.2f %s", dsize, units[unit_index]);
    return buf;
}

// ----- 再帰的ディレクトリ作成関数 -----
// 指定されたパスのディレクトリが存在しなければ、親ディレクトリも含めて再帰的に作成する
void create_directory_recursive(const char *path) {
    if (GetFileAttributes(path) != INVALID_FILE_ATTRIBUTES)
        return; // すでに存在する
    char parent[MAX_PATH];
    strcpy(parent, path);
    char *lastSlash = strrchr(parent, '\\');
    if (lastSlash != NULL) {
        *lastSlash = '\0';
        create_directory_recursive(parent);
    }
    if (!CreateDirectory(path, NULL)) {
        printf("エラー: コピー先フォルダ %s の作成に失敗しました。\n", path);
        exit(1);
    } else {
        printf("コピー先フォルダ %s を作成しました。\n", path);
    }
}

// ----- ディスク・フォルダ関数 -----
// 指定パスの空き容量（バイト単位）を取得（Windows API）
unsigned long long get_free_space(const char *path) {
    ULARGE_INTEGER freeBytesAvailable, totalBytes, totalFree;
    if (GetDiskFreeSpaceEx(path, &freeBytesAvailable, &totalBytes, &totalFree))
        return freeBytesAvailable.QuadPart;
    return 0;
}
// 指定フォルダのサイズ（バイト単位）を再帰的に計算する
unsigned long long get_folder_size(const char *path) {
    WIN32_FIND_DATA findFileData;
    HANDLE hFind;
    char searchPath[MAX_PATH];
    unsigned long long totalSize = 0;
    
    snprintf(searchPath, sizeof(searchPath), "%s\\*", path);
    hFind = FindFirstFile(searchPath, &findFileData);
    if (hFind == INVALID_HANDLE_VALUE)
        return 0;
    
    do {
        if (strcmp(findFileData.cFileName, ".") == 0 ||
            strcmp(findFileData.cFileName, "..") == 0)
            continue;
        char fullPath[MAX_PATH];
        snprintf(fullPath, sizeof(fullPath), "%s\\%s", path, findFileData.cFileName);
        if (findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
            totalSize += get_folder_size(fullPath);
        else
            totalSize += (((unsigned long long)findFileData.nFileSizeHigh << 32) | findFileData.nFileSizeLow);
    } while (FindNextFile(hFind, &findFileData) != 0);
    
    FindClose(hFind);
    return totalSize;
}

// ----- ファイル比較・新規ファイル名生成 -----
// 2つのファイルが同一かどうかを判定する（同一なら非0、異なれば0）
int files_are_identical(const char *file1, const char *file2) {
    FILE *fp1 = fopen(file1, "rb");
    FILE *fp2 = fopen(file2, "rb");
    if (!fp1 || !fp2) {
        if (fp1) fclose(fp1);
        if (fp2) fclose(fp2);
        return 0;
    }
    fseek(fp1, 0, SEEK_END);
    fseek(fp2, 0, SEEK_END);
    long size1 = ftell(fp1);
    long size2 = ftell(fp2);
    if (size1 != size2) {
        fclose(fp1);
        fclose(fp2);
        return 0;
    }
    rewind(fp1);
    rewind(fp2);
    
    char buffer1[BUFFER_SIZE], buffer2[BUFFER_SIZE];
    size_t bytes1, bytes2;
    int identical = 1;
    do {
        bytes1 = fread(buffer1, 1, BUFFER_SIZE, fp1);
        bytes2 = fread(buffer2, 1, BUFFER_SIZE, fp2);
        if (bytes1 != bytes2 || memcmp(buffer1, buffer2, bytes1) != 0) {
            identical = 0;
            break;
        }
    } while (bytes1 > 0);
    
    fclose(fp1);
    fclose(fp2);
    return identical;
}
// 新しいファイル名を生成する（例："file.txt" -> "file_copy.txt"）
void generate_new_filename(const char *dest, char *new_dest, size_t new_dest_size) {
    strncpy(new_dest, dest, new_dest_size);
    new_dest[new_dest_size - 1] = '\0';
    char *dot = strrchr(new_dest, '.');
    if (dot) {
        size_t pos = dot - new_dest;
        char temp[MAX_PATH];
        strncpy(temp, new_dest, pos);
        temp[pos] = '\0';
        strncat(temp, "_copy", sizeof(temp) - strlen(temp) - 1);
        strncat(temp, dot, sizeof(temp) - strlen(temp) - 1);
        strncpy(new_dest, temp, new_dest_size);
    } else {
        strncat(new_dest, "_copy", new_dest_size - strlen(new_dest) - 1);
    }
}

// ----- 文字列置換 -----
// 指定されたパス中に search が含まれていれば、replace に置換する
int rename_file_by_replacement(const char *path, const char *search, const char *replace) {
    if (strlen(search) == 0)
        return 0;
    const char *pos = strstr(path, search);
    if (!pos)
        return 0;
    char new_name[MAX_PATH];
    size_t prefix_len = pos - path;
    strncpy(new_name, path, prefix_len);
    new_name[prefix_len] = '\0';
    strncat(new_name, replace, sizeof(new_name) - strlen(new_name) - 1);
    strncat(new_name, pos + strlen(search), sizeof(new_name) - strlen(new_name) - 1);
    if (!MoveFile(path, new_name)) {
        printf("エラー: %s の置換に失敗しました。\n", path);
        return -1;
    }
    printf("置換により、名前を %s に変更しました。\n", new_name);
    log_message("名前変更: %s -> %s\n", path, new_name);
    return 0;
}

// ----- コピー・削除処理 -----
// ファイルの場合：
//   - 宛先が存在し、内容が同一なら、
//       g_deleteSource 有効ならコピーせずソース削除、無効なら保持。
//   - 宛先が存在し、内容が異なるなら、
//       g_deleteSource 有効なら "_copy" 付加でコピー＆削除、無効ならコピーのみ。
//   - 宛先が存在しなければ通常コピーし、g_deleteSource に応じて削除または保持。
// ※ オプションが "d" でなければ、ファイル名置換処理を実施する。
int copy_or_delete_file(const char *src, const char *dest, const char *search, const char *replace) {
    if (GetFileAttributes(dest) != INVALID_FILE_ATTRIBUTES) {
        if (files_are_identical(src, dest)) {
            if (g_deleteSource) {
                if (DeleteFile(src)) {
                    printf("\n[同一ファイル] %s -> %s : コピーせずソース削除\n", src, dest);
                    log_message("%s -> %s: 同一ファイル、ソース削除\n", src, dest);
                    if (strcmp(search, "d") != 0)
                        rename_file_by_replacement(dest, search, replace);
                    return 0;
                } else {
                    printf("\nエラー: %s の削除に失敗しました。\n", src);
                    log_message("%s -> %s: 削除失敗\n", src, dest);
                    return -1;
                }
            } else {
                printf("\n[同一ファイル] %s と %s は同一。ソース保持\n", src, dest);
                log_message("%s -> %s: 同一ファイル、ソース保持\n", src, dest);
                return 0;
            }
        } else {
            char new_dest[MAX_PATH];
            generate_new_filename(dest, new_dest, sizeof(new_dest));
            if (!CopyFile(src, new_dest, FALSE)) {
                printf("\nエラー: %s を %s にコピーできませんでした。\n", src, new_dest);
                log_message("%s -> %s: コピー失敗\n", src, new_dest);
                return -1;
            }
            if (g_deleteSource) {
                if (!DeleteFile(src)) {
                    printf("\nエラー: %s の削除に失敗しました。\n", src);
                    log_message("%s -> %s: 削除失敗\n", src, new_dest);
                    return -1;
                }
            }
            printf("\n[異なるファイル] %s を %s としてコピー%s\n", src, new_dest, g_deleteSource ? "、ソース削除" : "（ソース保持）");
            log_message("%s -> %s: 異なるファイル、%s\n", src, new_dest, g_deleteSource ? "ソース削除" : "ソース保持");
            if (strcmp(search, "d") != 0)
                rename_file_by_replacement(new_dest, search, replace);
            return 0;
        }
    } else {
        if (!CopyFile(src, dest, FALSE)) {
            printf("\nエラー: %s を %s にコピーできませんでした。\n", src, dest);
            log_message("%s -> %s: コピー失敗\n", src, dest);
            return -1;
        }
        if (g_deleteSource) {
            if (!DeleteFile(src)) {
                printf("\nエラー: %s の削除に失敗しました。\n", src);
                log_message("%s -> %s: 削除失敗\n", src, dest);
                return -1;
            }
            printf("\n[新規コピー] %s を %s にコピー、ソース削除\n", src, dest);
            log_message("%s -> %s: 新規コピー、ソース削除\n", src, dest);
        } else {
            printf("\n[新規コピー] %s を %s にコピー、ソース保持\n", src, dest);
            log_message("%s -> %s: 新規コピー、ソース保持\n", src, dest);
        }
        if (strcmp(search, "d") != 0)
            rename_file_by_replacement(dest, search, replace);
        return 0;
    }
}

// ----- 再帰的コピー処理 -----
// src の内容を dest に再帰的にコピーする。
// 引数 search, replace はファイル名置換用。
// level: 階層（0=コピー元フォルダ自体、1=直下の項目、2以上=それ以降）。
// folder_option: 1 の場合、コピー元フォルダ直下のフォルダ名に対して置換処理を適用する。
int copy_folder_recursive(const char *src, const char *dest, unsigned long long total_size, const char *search, const char *replace, int level, int folder_option) {
    WIN32_FIND_DATA findFileData;
    HANDLE hFind;
    char searchPath[MAX_PATH];
    unsigned long long copied_size = 0;
    
    snprintf(searchPath, sizeof(searchPath), "%s\\*", src);
    hFind = FindFirstFile(searchPath, &findFileData);
    if (hFind == INVALID_HANDLE_VALUE)
        return -1;
    
    // コピー先フォルダが存在しなければ再帰的に作成
    create_directory_recursive(dest);
    
    do {
        if (strcmp(findFileData.cFileName, ".") == 0 ||
            strcmp(findFileData.cFileName, "..") == 0)
            continue;
        char srcPath[MAX_PATH], destPath[MAX_PATH];
        snprintf(srcPath, sizeof(srcPath), "%s\\%s", src, findFileData.cFileName);
        snprintf(destPath, sizeof(destPath), "%s\\%s", dest, findFileData.cFileName);
        
        if (findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
            copy_folder_recursive(srcPath, destPath, total_size, search, replace, level + 1, folder_option);
            if (level == 0 && folder_option == 1)
                rename_file_by_replacement(destPath, search, replace);
            if (g_deleteSource) {
                if (!RemoveDirectory(srcPath))
                    printf("エラー: ディレクトリ %s の削除に失敗しました。\n", srcPath);
                else
                    printf("\n[フォルダ削除] %s を削除しました。\n", srcPath);
            }
        } else {
            if (copy_or_delete_file(srcPath, destPath, search, replace) == 0) {
                copied_size += (((unsigned long long)findFileData.nFileSizeHigh << 32) | findFileData.nFileSizeLow);
                printf("\r進捗: %.2f%%", (double)copied_size / total_size * 100);
                fflush(stdout);
            }
        }
    } while (FindNextFile(hFind, &findFileData) != 0);
    
    FindClose(hFind);
    if (g_deleteSource) {
        if (!RemoveDirectory(src))
            printf("\nエラー: ソースフォルダ %s の削除に失敗しました。\n", src);
        else
            printf("\n[フォルダ削除] ソースフォルダ %s を削除しました。\n", src);
    }
    return 0;
}

// ----- 履歴読み込み -----
// history.txt の各行は
// "コピー元, コピー先, 置換前文字列, 置換後文字列, オプション"
// の形式で記述（オプションが "d" ならコピー元直下のフォルダ名に対して置換処理を適用）
int load_history(char src_list[MAX_ENTRIES][MAX_PATH], char dest_list[MAX_ENTRIES][MAX_PATH],
                 char replace_from[MAX_ENTRIES][MAX_REPLACE_LEN], char replace_to[MAX_ENTRIES][MAX_REPLACE_LEN],
                 char rep_option[MAX_ENTRIES][MAX_REPLACE_LEN]) {
    FILE *file = fopen(HISTORY_FILE, "r");
    if (!file)
        return 0;
    int count = 0;
    char line[BUFFER_SIZE];
    while (fgets(line, sizeof(line), file)) {
        line[strcspn(line, "\n")] = '\0';
        char *token = strtok(line, ",");
        if (token == NULL) continue;
        strncpy(src_list[count], token, MAX_PATH);
        src_list[count][MAX_PATH - 1] = '\0';
        trim(src_list[count]);
        
        token = strtok(NULL, ",");
        if (token == NULL) continue;
        strncpy(dest_list[count], token, MAX_PATH);
        dest_list[count][MAX_PATH - 1] = '\0';
        trim(dest_list[count]);
        
        token = strtok(NULL, ",");
        if (token != NULL) {
            strncpy(replace_from[count], token, MAX_REPLACE_LEN);
            replace_from[count][MAX_REPLACE_LEN - 1] = '\0';
            trim(replace_from[count]);
        } else {
            replace_from[count][0] = '\0';
        }
        
        token = strtok(NULL, ",");
        if (token != NULL) {
            strncpy(replace_to[count], token, MAX_REPLACE_LEN);
            replace_to[count][MAX_REPLACE_LEN - 1] = '\0';
            trim(replace_to[count]);
        } else {
            replace_to[count][0] = '\0';
        }
        
        token = strtok(NULL, ",");
        if (token != NULL) {
            strncpy(rep_option[count], token, MAX_REPLACE_LEN);
            rep_option[count][MAX_REPLACE_LEN - 1] = '\0';
            trim(rep_option[count]);
        } else {
            rep_option[count][0] = '\0';
        }
        
        count++;
        if (count >= MAX_ENTRIES)
            break;
    }
    fclose(file);
    return count;
}

// ----- スレッド関数 -----
// 各コピータスクをスレッドで実行する関数
DWORD WINAPI CopyThreadFunc(LPVOID lpParam) {
    CopyTask *task = (CopyTask*)lpParam;
    time_t startTime = time(NULL);
    printf("\n[タスク %d] コピー開始: %s -> %s\n", task->task_id, task->src, task->dest);
    log_message("[タスク %d] コピー開始: %s -> %s, 開始時刻: %s", task->task_id, task->src, task->dest, ctime(&startTime));
    int folder_option = (strcmp(task->replace_option, "d") == 0) ? 1 : 0;
    copy_folder_recursive(task->src, task->dest, task->folder_size, task->replace_from, task->replace_to, 0, folder_option);
    time_t endTime = time(NULL);
    printf("\n[タスク %d] コピー完了！\n", task->task_id);
    log_message("[タスク %d] コピー完了: %s -> %s, 終了時刻: %s\n", task->task_id, task->src, task->dest, ctime(&endTime));
    return 0;
}

// ----- 実行日時待機処理 -----
// schedule.txt に "YYYY-MM-DD HH:MM:SS" 形式で指定された日時まで、
// 日、時間、分、秒で残り時間をリアルタイムに表示しながら待機する。
void wait_until_scheduled_time() {
    FILE *fp = fopen(SCHEDULE_FILE, "r");
    if (fp) {
        char scheduleStr[64];
        if (fgets(scheduleStr, sizeof(scheduleStr), fp)) {
            scheduleStr[strcspn(scheduleStr, "\n")] = '\0';
            struct tm tmSchedule = {0};
            if (sscanf(scheduleStr, "%d-%d-%d %d:%d:%d",
                       &tmSchedule.tm_year, &tmSchedule.tm_mon, &tmSchedule.tm_mday,
                       &tmSchedule.tm_hour, &tmSchedule.tm_min, &tmSchedule.tm_sec) == 6) {
                tmSchedule.tm_year -= 1900;
                tmSchedule.tm_mon -= 1;
                time_t scheduledTime = mktime(&tmSchedule);
                time_t now;
                while ((now = time(NULL)) < scheduledTime) {
                    int remaining = (int)(scheduledTime - now);
                    int days = remaining / (24 * 3600);
                    int hours = (remaining % (24 * 3600)) / 3600;
                    int minutes = (remaining % 3600) / 60;
                    int seconds = remaining % 60;
                    printf("\r残り時間: %d日 %d時間 %d分 %d秒  ", days, hours, minutes, seconds);
                    fflush(stdout);
                    Sleep(1000);
                }
                printf("\n指定時刻になりました。\n");
            }
        }
        fclose(fp);
    }
}

// ----- メイン関数 -----
// 処理の順序は以下の通り：
// 0. アプリ実行
// 1. コピー完了後にコピー元削除の確認
// 2. 一斉実行か個別確認かの選択
// 3. 指定日時まで待機（残り時間をリアルタイム表示）
// 4. 指定時刻になったらタスク実行
// 5. 終了
int main() {
    // コンソール出力コードページをUTF-8に設定
    SetConsoleOutputCP(CP_UTF8);
    
    // ログ用ミューテックス作成
    g_logMutex = CreateMutex(NULL, FALSE, NULL);
    
    // ① コピー完了後にコピー元の削除確認
    char user_choice;
    printf("コピー完了後にコピー元のフォルダ/ファイルを削除しますか？ (Y/N): ");
    scanf(" %c", &user_choice);
    g_deleteSource = (user_choice == 'Y' || user_choice == 'y') ? 1 : 0;
    
    // ② 一斉実行か個別確認かの選択
    printf("すべてのタスクを一斉に開始しますか？ (Y: 一斉実行 / N: 個別確認): ");
    scanf(" %c", &user_choice);
    int all_mode = (user_choice == 'Y' || user_choice == 'y') ? 1 : 0;
    
    // ③ 指定日時まで待機（残り時間をリアルタイムに表示）
    wait_until_scheduled_time();
    
    // グローバル開始時刻のログ
    time_t globalStart = time(NULL);
    log_message("=== 実行開始時刻: %s", ctime(&globalStart));
    
    char src_list[MAX_ENTRIES][MAX_PATH], dest_list[MAX_ENTRIES][MAX_PATH];
    char replace_from[MAX_ENTRIES][MAX_REPLACE_LEN], replace_to[MAX_ENTRIES][MAX_REPLACE_LEN];
    char rep_option[MAX_ENTRIES][MAX_REPLACE_LEN];
    int history_count = load_history(src_list, dest_list, replace_from, replace_to, rep_option);
    
    char src_size_buf[64], dest_size_buf[64];  // サイズ表示用のバッファ（別々）
    
    if (history_count == 0) {
        printf("履歴が見つかりませんでした。\n");
        return 0;
    }
    
    if (all_mode) {
        CopyTask tasks[MAX_ENTRIES];
        int task_count = 0;
        for (int i = 0; i < history_count; i++) {
            create_directory_recursive(dest_list[i]);
            unsigned long long folder_size = get_folder_size(src_list[i]);
            unsigned long long free_space = get_free_space(dest_list[i]);
            
            printf("\n[%d] コピー元: %s\n", i + 1, src_list[i]);
            printf("[%d] コピー先: %s\n", i + 1, dest_list[i]);
            printf("コピー元サイズ: %s, コピー先空き容量: %s\n",
                   format_size(folder_size, src_size_buf, sizeof(src_size_buf)),
                   format_size(free_space, dest_size_buf, sizeof(dest_size_buf)));
            
            if (folder_size > free_space) {
                printf("エラー: 空き容量が不足しています。このタスクはスキップします。\n");
                continue;
            }
            
            strcpy(tasks[task_count].src, src_list[i]);
            strcpy(tasks[task_count].dest, dest_list[i]);
            tasks[task_count].folder_size = folder_size;
            tasks[task_count].task_id = task_count + 1;
            strcpy(tasks[task_count].replace_from, replace_from[i]);
            strcpy(tasks[task_count].replace_to, replace_to[i]);
            strcpy(tasks[task_count].replace_option, rep_option[i]);
            task_count++;
        }
        
        if (task_count == 0) {
            printf("\n実行するコピータスクはありませんでした。\n");
            return 0;
        }
        
        printf("\nすべてのタスクのチェックが完了しました。一斉にコピーを開始します。\n");
        HANDLE threads[MAX_ENTRIES];
        for (int i = 0; i < task_count; i++) {
            threads[i] = CreateThread(NULL, 0, CopyThreadFunc, &tasks[i], 0, NULL);
            if (threads[i] == NULL) {
                printf("エラー: タスク %d のスレッド作成に失敗しました。\n", tasks[i].task_id);
            }
        }
        WaitForMultipleObjects(task_count, threads, TRUE, INFINITE);
        for (int i = 0; i < task_count; i++) {
            CloseHandle(threads[i]);
        }
        printf("\nすべてのコピータスクが完了しました！\n");
        
    } else {
        for (int i = 0; i < history_count; i++) {
            printf("\n[%d] コピー元: %s\n", i + 1, src_list[i]);
            printf("[%d] コピー先: %s\n", i + 1, dest_list[i]);
            printf("このコピーを実行しますか？ (Y/N): ");
            scanf(" %c", &user_choice);
            if (user_choice != 'Y' && user_choice != 'y') {
                printf("このコピーはスキップされました。\n");
                continue;
            }
            create_directory_recursive(dest_list[i]);
            unsigned long long folder_size = get_folder_size(src_list[i]);
            unsigned long long free_space = get_free_space(dest_list[i]);
            printf("コピー元サイズ: %s, コピー先空き容量: %s\n",
                   format_size(folder_size, src_size_buf, sizeof(src_size_buf)),
                   format_size(free_space, dest_size_buf, sizeof(dest_size_buf)));
            if (folder_size > free_space) {
                printf("エラー: 空き容量が不足しています。このコピーはスキップされます。\n");
                continue;
            }
            printf("コピーを開始します...\n");
            int folder_option = (strcmp(rep_option[i], "d") == 0) ? 1 : 0;
            copy_folder_recursive(src_list[i], dest_list[i], folder_size, replace_from[i], replace_to[i], 0, folder_option);
            printf("\nコピー完了！\n");
        }
    }
    
    time_t globalEnd = time(NULL);
    log_message("=== 実行終了時刻: %s\n", ctime(&globalEnd));
    
    CloseHandle(g_logMutex);
    return 0;
}
