export function resolveQcPhotoUrl(photoUrl) {
  if (!photoUrl) return photoUrl;

  if (photoUrl.startsWith('/uploads/qc/')) {
    return photoUrl;
  }

  if (photoUrl.includes('/uploads/qc/')) {
    const filename = photoUrl.split('/uploads/qc/').pop();
    return filename ? `/uploads/qc/${filename}` : photoUrl;
  }

  if (photoUrl.includes('/uploads/')) {
    const filename = photoUrl.split('/uploads/').pop()?.split('/').pop();
    return filename ? `/uploads/qc/${filename}` : photoUrl;
  }

  return photoUrl;
}
