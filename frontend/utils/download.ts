// adapted from https://stackoverflow.com/questions/48760815/export-to-csv-button-in-react-table
const rowsToCsv = (rows: any[]) => {
  if (!rows) {
    return;
  }

  const separator: string = ";";
  const keys: string[] = Object.keys(rows[0]);

  const csvContent = `${keys.join(separator)}\n${rows
    .map((row) =>
      keys
        .map((k) => {
          let cell = row[k] === null || row[k] === undefined ? "" : row[k];

          if (cell instanceof Date) {
            cell = cell.toLocaleString();
          } else if (cell instanceof Object) {
            cell = JSON.stringify(cell);
          } else {
            cell = cell.toString().replace(/"/g, '""');
          }

          if (cell.search(/("|,|\n)/g) >= 0) {
            cell = `"${cell}"`;
          }
          return cell;
        })
        .join(separator)
    )
    .join("\n")}`;

  return csvContent;
};

// adapted from https://stackoverflow.com/questions/48760815/export-to-csv-button-in-react-table
const download = async (files: any[], zipName: string) => {
  // let zip = new JSZip();

  // // add each file to the archive
  // files.forEach((file: any) => {
  //   const { fileName, content } = file;
  //   if (fileName && content) {
  //     zip.file(fileName, content);
  //   }
  // });

  // // generate blob
  // let blob = await zip.generateAsync({ type: "blob" });
  const link = document.createElement("a");

  // html5 download attribute (we assume all our users have html5+)
  if (link.download !== undefined) {
    const url = URL.createObjectURL(new Blob(files[0].content));
    link.setAttribute("href", url);
    link.setAttribute("download", zipName);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }
};

export { rowsToCsv, download };
