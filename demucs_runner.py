"""
Demucs wrapper that ensures torchaudio backend compatibility.
Fixes: ImportError: TorchCodec is required for load_with_torchcodec

This wrapper sets the torchaudio backend to 'soundfile' before running Demucs,
which avoids the torchcodec dependency in newer torchaudio versions (2.5+).
"""
import sys

# Fix torchaudio backend BEFORE importing demucs
try:
    import torchaudio
    # Try to set backend to soundfile (works without torchcodec)
    try:
        torchaudio.set_audio_backend("soundfile")
    except Exception:
        pass
    
    # For torchaudio >= 2.5, patch load function to use soundfile fallback
    _original_load = torchaudio.load
    def _patched_load(uri, *args, **kwargs):
        try:
            return _original_load(uri, *args, **kwargs)
        except ImportError as e:
            if 'torchcodec' in str(e).lower():
                # Fallback: use soundfile directly
                import soundfile as sf
                import torch
                data, sample_rate = sf.read(str(uri), dtype='float32')
                # Convert to torch tensor (channels, samples)
                if data.ndim == 1:
                    tensor = torch.from_numpy(data).unsqueeze(0)
                else:
                    tensor = torch.from_numpy(data.T)
                return tensor, sample_rate
            raise
    torchaudio.load = _patched_load
except ImportError:
    pass

# Now run demucs normally
from demucs.separate import main
sys.exit(main())
