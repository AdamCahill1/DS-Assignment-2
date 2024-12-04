export function isImageCorrect(srcKey: string): boolean {
    return srcKey.endsWith(".jpg") || srcKey.endsWith(".png") || srcKey.endsWith(".jpeg");
}
  